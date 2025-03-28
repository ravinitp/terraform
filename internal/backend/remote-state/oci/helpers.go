package oci

import (
	"bytes"
	"context"
	"fmt"
	"github.com/hashicorp/terraform/internal/backend/backendbase"
	"io"
	"sync"

	"github.com/oracle/oci-go-sdk/v65/common"
	oci_object_storage "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

const DefaultFilePartSize int64 = 128 * 1024 * 1024 // 128MB
const defaultNumberOfGoroutines = 10
const MaxPartSize int64 = 50 * 1024 * 1024 * 1024
const MaxCount int64 = 10000

type MultipartUploadData struct {
	NamespaceName       *string
	BucketName          *string
	ObjectName          *string
	ObjectStorageClient *oci_object_storage.ObjectStorageClient
	Data                []byte
	StorageTier         oci_object_storage.StorageTierEnum
	Metadata            map[string]string
	RequestMetadata     common.RequestMetadata
}

type objectStorageUploadPartResponse struct {
	response   oci_object_storage.UploadPartResponse
	partNumber *int
	error      error
}

type objectStorageMultiPartUploadContext struct {
	client                  oci_object_storage.ObjectStorageClient
	sourceBlocks            chan objectStorageSourceBlock
	osUploadPartResponses   chan objectStorageUploadPartResponse
	wg                      *sync.WaitGroup
	multipartUploadResponse oci_object_storage.CreateMultipartUploadResponse
	multipartUploadRequest  oci_object_storage.CreateMultipartUploadRequest
}

type objectStorageSourceBlock struct {
	section     *io.SectionReader
	blockNumber *int
}

func multiPartUpload(multipartUploadData MultipartUploadData) (string, error) {
	dataSize := int64(len(multipartUploadData.Data))
	if dataSize > DefaultFilePartSize {
		return multiPartUploadImpl(multipartUploadData)
	}
	// return "", fmt.Errorf("data size too small for multipart upload")
}

func multiPartUploadImpl(multipartUploadData MultipartUploadData) (string, error) {
	client := multipartUploadData.ObjectStorageClient

	sourceBlocks, err := objectMultiPartSplit(multipartUploadData.Data)
	if err != nil {
		return "", fmt.Errorf("error splitting source data: %s", err)
	}

	multipartUploadRequest := &oci_object_storage.CreateMultipartUploadRequest{
		NamespaceName:   multipartUploadData.NamespaceName,
		BucketName:      multipartUploadData.BucketName,
		RequestMetadata: multipartUploadData.RequestMetadata,
		CreateMultipartUploadDetails: oci_object_storage.CreateMultipartUploadDetails{
			Object:      multipartUploadData.ObjectName,
			StorageTier: multipartUploadData.StorageTier,
			Metadata:    multipartUploadData.Metadata,
		},
	}

	multipartUploadResponse, err := client.CreateMultipartUpload(context.Background(), *multipartUploadRequest)
	if err != nil {
		return "", fmt.Errorf("error creating multipart upload: %s", err)
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

	// Start workers
	for i := 0; i < workerCount; i++ {
		go func() {
			err := uploadPartsWorker(objectStorageMultiPartUploadContext{
				client:                  *client,
				wg:                      wg,
				multipartUploadResponse: multipartUploadResponse,
				multipartUploadRequest:  *multipartUploadRequest,
				sourceBlocks:            sourceBlocksChan,
				osUploadPartResponses:   osUploadPartResponses,
			})
			if err != nil {
				backendbase.ErrorAsDiagnostics(err)
			}
		}()
	}

	wg.Wait()
	close(osUploadPartResponses)

	commitMultipartUploadPartDetails := make([]oci_object_storage.CommitMultipartUploadPartDetails, len(sourceBlocks))
	i := 0
	for response := range osUploadPartResponses {
		if response.error != nil {
			return "", fmt.Errorf("failed to upload part: %s", response.error)
		}
		commitMultipartUploadPartDetails[i] = oci_object_storage.CommitMultipartUploadPartDetails{
			PartNum: response.partNumber,
			Etag:    response.response.ETag,
		}
		i++
	}

	commitMultipartUploadRequest := oci_object_storage.CommitMultipartUploadRequest{
		UploadId:           multipartUploadResponse.MultipartUpload.UploadId,
		NamespaceName:      multipartUploadResponse.Namespace,
		BucketName:         multipartUploadResponse.Bucket,
		ObjectName:         multipartUploadResponse.Object,
		OpcClientRequestId: multipartUploadResponse.OpcClientRequestId,
		RequestMetadata:    multipartUploadRequest.RequestMetadata,
	}

	_, err = client.CommitMultipartUpload(context.Background(), commitMultipartUploadRequest)
	if err != nil {
		return "", fmt.Errorf("failed to commit multipart upload: %s", err)
	}

	return "Upload successful", nil
}
func objectMultiPartSplit(data []byte) ([]objectStorageSourceBlock, error) {
	dataSize := int64(len(data))
	offsets, limits, _ := SplitSizeToOffsetsAndLimits(dataSize)

	sourceBlocks := make([]objectStorageSourceBlock, len(offsets))
	for i := range offsets {
		start := offsets[i]
		end := start + limits[i]
		if end > dataSize {
			end = dataSize
		}
		sourceBlocks[i] = objectStorageSourceBlock{
			section:     io.NewSectionReader(bytes.NewReader(data), start, end-start),
			blockNumber: &i,
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

func uploadPartsWorker(ctx objectStorageMultiPartUploadContext) error {
	for block := range ctx.sourceBlocks {
		buffer := make([]byte, block.section.Size())
		_, err := block.section.Read(buffer)
		if err != nil {
			return fmt.Errorf("error reading source block %d: %w", block.blockNumber, err)
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

		_, err := ctx.client.UploadPart(context.Background(), *uploadPartRequest)
		ctx.wg.Done()
		if err != nil {
			logger.Error("Failed to upload part #%d: %s", block.blockNumber, err)
			return err
		}
	}
	return nil
}

func DeleteAllObjectVersions(client *oci_object_storage.ObjectStorageClient, bucket string, namespace string, prefix string) error {
	request := oci_object_storage.ListObjectVersionsRequest{}

	request.BucketName = &bucket
	request.NamespaceName = &namespace

	if prefix != "" {
		request.Prefix = &prefix
	}

	response, err := client.ListObjectVersions(context.Background(), request)
	if err != nil {
		return err
	}

	request.Page = response.OpcNextPage

	for request.Page != nil {
		request.RequestMetadata.RetryPolicy = getDefaultRetryPolicy()

		listResponse, err := client.ListObjectVersions(context.Background(), request)
		if err != nil {
			return err
		}
		response.Items = append(response.Items, listResponse.Items...)
		request.Page = listResponse.OpcNextPage
	}

	var errors []string
	for _, objectVersion := range response.Items {

		deleteObjectVersionRequest := oci_object_storage.DeleteObjectRequest{}
		deleteObjectVersionRequest.BucketName = &bucket
		deleteObjectVersionRequest.NamespaceName = &namespace
		deleteObjectVersionRequest.ObjectName = objectVersion.Name
		deleteObjectVersionRequest.VersionId = objectVersion.VersionId

		deleteObjectVersionRequest.RequestMetadata.RetryPolicy = getDefaultRetryPolicy()

		_, err := client.DeleteObject(context.Background(), deleteObjectVersionRequest)
		if err != nil {
			errors = append(errors, err.Error())
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("%v", errors)
	}

	return nil
}
