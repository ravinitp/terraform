package oci

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/oracle/oci-go-sdk/v65/common"
	oci_object_storage "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

const DefaultFilePartSize int64 = 64 * 1024 // 128 * 1024 * 1024 // 128MB
const defaultNumberOfGoroutines = 10
const MaxPartSize int64 = 50 * 1024 * 1024 * 1024
const MaxCount int64 = 10000

type MultipartUploadData struct {
	client          *RemoteClient
	Data            []byte
	RequestMetadata common.RequestMetadata
}

type objectStorageUploadPartResponse struct {
	response   oci_object_storage.UploadPartResponse
	partNumber *int
	error      error
}

type objectStorageMultiPartUploadContext struct {
	client                  *oci_object_storage.ObjectStorageClient
	sourceBlocks            chan objectStorageSourceBlock
	osUploadPartResponses   chan objectStorageUploadPartResponse
	wg                      *sync.WaitGroup
	errChan                 chan error
	multipartUploadResponse oci_object_storage.CreateMultipartUploadResponse
	multipartUploadRequest  oci_object_storage.CreateMultipartUploadRequest
}

type objectStorageSourceBlock struct {
	section     *io.SectionReader
	blockNumber *int
}

func (multipartUploadData MultipartUploadData) multiPartUploadImpl() error {

	sourceBlocks, err := multipartUploadData.objectMultiPartSplit()
	if err != nil {
		return fmt.Errorf("error splitting source data: %s", err)
	}

	multipartUploadRequest := &oci_object_storage.CreateMultipartUploadRequest{
		NamespaceName:   common.String(multipartUploadData.client.namespace),
		BucketName:      common.String(multipartUploadData.client.bucketName),
		RequestMetadata: multipartUploadData.RequestMetadata,
		CreateMultipartUploadDetails: oci_object_storage.CreateMultipartUploadDetails{
			Object: common.String(multipartUploadData.client.path),
		},
	}

	multipartUploadResponse, err := multipartUploadData.client.objectStorageClient.CreateMultipartUpload(context.Background(), *multipartUploadRequest)
	if err != nil {
		return fmt.Errorf("error creating multipart upload: %s", err)
	}

	workerCount := defaultNumberOfGoroutines
	osUploadPartResponses := make(chan objectStorageUploadPartResponse, len(sourceBlocks))
	sourceBlocksChan := make(chan objectStorageSourceBlock, len(sourceBlocks))

	wg := &sync.WaitGroup{}
	wg.Add(len(sourceBlocks))

	// Push all source blocks into the channel
	for _, sourceBlock := range sourceBlocks {
		sourceBlocksChan <- sourceBlock
	}
	close(sourceBlocksChan)
	errChan := make(chan error, workerCount)
	// Start workers
	for i := 0; i < workerCount; i++ {
		go func() {
			ctx := &objectStorageMultiPartUploadContext{
				client:                  multipartUploadData.client.objectStorageClient,
				wg:                      wg,
				errChan:                 errChan,
				multipartUploadResponse: multipartUploadResponse,
				multipartUploadRequest:  *multipartUploadRequest,
				sourceBlocks:            sourceBlocksChan,
				osUploadPartResponses:   osUploadPartResponses,
			}
			ctx.uploadPartsWorker()
		}()
	}

	wg.Wait()
	close(osUploadPartResponses)
	close(errChan)

	// Collect errors from workers
	for workerErr := range errChan {
		if workerErr != nil {
			return workerErr
		}
	}
	commitMultipartUploadPartDetails := make([]oci_object_storage.CommitMultipartUploadPartDetails, len(sourceBlocks))
	i := 0
	for response := range osUploadPartResponses {
		if response.error != nil || response.partNumber == nil || response.response.ETag == nil {
			return fmt.Errorf("failed to upload part: %s", response.error)
		}
		partNumber, etag := *response.partNumber, *response.response.ETag
		commitMultipartUploadPartDetails[i] = oci_object_storage.CommitMultipartUploadPartDetails{
			PartNum: common.Int(partNumber),
			Etag:    common.String(etag),
		}
		i++
	}

	if len(commitMultipartUploadPartDetails) != len(sourceBlocks) {
		abortReq := oci_object_storage.AbortMultipartUploadRequest{
			UploadId:      multipartUploadResponse.MultipartUpload.UploadId,
			NamespaceName: multipartUploadResponse.Namespace,
			BucketName:    multipartUploadResponse.Bucket,
			ObjectName:    multipartUploadResponse.Object,
		}
		_, abortErr := multipartUploadData.client.objectStorageClient.AbortMultipartUpload(context.Background(), abortReq)
		if abortErr != nil {
			logger.Error(fmt.Sprintf("Failed to abort multipart upload: %s", abortErr))
		}
		return fmt.Errorf("not all parts uploaded successfully, multipart upload aborted")
	}

	commitMultipartUploadRequest := oci_object_storage.CommitMultipartUploadRequest{
		UploadId:           multipartUploadResponse.MultipartUpload.UploadId,
		NamespaceName:      multipartUploadResponse.Namespace,
		BucketName:         multipartUploadResponse.Bucket,
		ObjectName:         multipartUploadResponse.Object,
		OpcClientRequestId: multipartUploadResponse.OpcClientRequestId,
		RequestMetadata:    multipartUploadRequest.RequestMetadata,
		CommitMultipartUploadDetails: oci_object_storage.CommitMultipartUploadDetails{
			PartsToCommit: commitMultipartUploadPartDetails,
		},
	}
	_, err = multipartUploadData.client.objectStorageClient.CommitMultipartUpload(context.Background(), commitMultipartUploadRequest)
	if err != nil {
		return fmt.Errorf("failed to commit multipart upload: %s", err)
	}

	return nil
}
func (m MultipartUploadData) objectMultiPartSplit() ([]objectStorageSourceBlock, error) {
	dataSize := int64(len(m.Data))
	offsets, limits, _ := SplitSizeToOffsetsAndLimits(dataSize)

	sourceBlocks := make([]objectStorageSourceBlock, len(offsets))
	for i := range offsets {
		start := offsets[i]
		end := start + limits[i]
		if end > dataSize {
			end = dataSize
		}
		sourceBlocks[i] = objectStorageSourceBlock{
			section:     io.NewSectionReader(bytes.NewReader(m.Data), start, end-start),
			blockNumber: common.Int(i + 1),
		}
	}
	return sourceBlocks, nil
}

func SplitSizeToOffsetsAndLimits(size int64) ([]int64, []int64, error) {
	partSize := DefaultFilePartSize
	totalParts := (size + partSize - 1) / partSize
	if totalParts > MaxCount {
		return nil, nil, fmt.Errorf("file exceeds maximum part count")
	}
	offsets, limits := make([]int64, totalParts), make([]int64, totalParts)
	for i := range offsets {
		offsets[i] = int64(i) * partSize
		limits[i] = partSize
	}
	return offsets, limits, nil
}

func (ctx *objectStorageMultiPartUploadContext) uploadPartsWorker() {
	for block := range ctx.sourceBlocks {
		buffer := make([]byte, block.section.Size())
		_, err := block.section.Read(buffer)
		if err != nil {
			ctx.errChan <- fmt.Errorf("error reading source block %d: %w", block.blockNumber, err)
			return
		}
		tmpLength := int64(len(buffer))

		uploadPartRequest := &oci_object_storage.UploadPartRequest{
			UploadId:       ctx.multipartUploadResponse.UploadId,
			ObjectName:     ctx.multipartUploadResponse.Object,
			NamespaceName:  ctx.multipartUploadResponse.Namespace,
			BucketName:     ctx.multipartUploadResponse.Bucket,
			ContentLength:  &tmpLength,
			UploadPartBody: io.NopCloser(bytes.NewReader(buffer)),
			UploadPartNum:  block.blockNumber,
			RequestMetadata: common.RequestMetadata{
				RetryPolicy: getDefaultRetryPolicy(),
			},
		}

		response, err := ctx.client.UploadPart(context.Background(), *uploadPartRequest)
		if err != nil {
			ctx.errChan <- fmt.Errorf("failed to upload part %d: %w", *block.blockNumber, err)
			return
		}
		ctx.osUploadPartResponses <- objectStorageUploadPartResponse{
			response:   response,
			error:      nil,
			partNumber: block.blockNumber,
		}
		ctx.wg.Done()

	}
}
