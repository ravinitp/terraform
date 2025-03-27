package oci

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/terraform/internal/states/remote"
	"github.com/hashicorp/terraform/internal/states/statemgr"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"io"
)

type RemoteClient struct {
	objectStorageClient         *objectstorage.ObjectStorageClient
	namespace                   string
	bucketName                  string
	path                        string
	lockFilePath                string
	customerEncryptionKey       []byte
	customerEncryptionKeySHA256 []byte
	encryptionAlgorithm         string
	kmsKeyID                    string
}

func (c *RemoteClient) Get() (*remote.Payload, error) {
	ctx := context.TODO()

	logger.Debug("Downloading remote state")

	return c.getObject(ctx)
}

func (c *RemoteClient) getObject(ctx context.Context) (*remote.Payload, error) {
	headRequest := objectstorage.HeadObjectRequest{
		NamespaceName: common.String(c.namespace),
		ObjectName:    common.String(c.path),
		BucketName:    common.String(c.bucketName),
		RequestMetadata: common.RequestMetadata{
			RetryPolicy: getDefaultRetryPolicy(),
		},
	}
	// Handle encryption settings
	if c.customerEncryptionKey != nil {
		if len(c.customerEncryptionKey) > 0 && len(c.customerEncryptionKeySHA256) > 0 {
			headRequest.OpcSseCustomerKey = common.String(base64.StdEncoding.EncodeToString(c.customerEncryptionKey))
			headRequest.OpcSseCustomerKeySha256 = common.String(base64.StdEncoding.EncodeToString(c.customerEncryptionKeySHA256))
		}
		if len(c.encryptionAlgorithm) > 0 {
			headRequest.OpcSseCustomerAlgorithm = common.String(c.encryptionAlgorithm)
		}
	}
	// Get object from OCI
	headResponse, headErr := c.objectStorageClient.HeadObject(ctx, headRequest)
	if headErr != nil {
		var ociHeadErr common.ServiceError
		if errors.As(headErr, &ociHeadErr) && ociHeadErr.GetHTTPStatusCode() == 404 {
			logger.Debug(" State file '%s' not found. Initializing Terraform state...", c.path)
			return nil, nil
		} else {
			return nil, fmt.Errorf("failed to access object '%s' in bucket '%s': %w", c.path, c.bucketName, headErr)
		}
	}
	getRequest := objectstorage.GetObjectRequest{
		NamespaceName: common.String(c.namespace),
		ObjectName:    common.String(c.path),
		BucketName:    common.String(c.bucketName),
		IfMatch:       headResponse.ETag,
		RequestMetadata: common.RequestMetadata{
			RetryPolicy: getDefaultRetryPolicy(),
		},
	}

	// Handle encryption settings
	if c.customerEncryptionKey != nil {
		if len(c.customerEncryptionKey) > 0 && len(c.customerEncryptionKeySHA256) > 0 {
			getRequest.OpcSseCustomerKey = common.String(base64.StdEncoding.EncodeToString(c.customerEncryptionKey))
			getRequest.OpcSseCustomerKeySha256 = common.String(base64.StdEncoding.EncodeToString(c.customerEncryptionKeySHA256))
		}
		if len(c.encryptionAlgorithm) > 0 {
			getRequest.OpcSseCustomerAlgorithm = common.String(c.encryptionAlgorithm)
		}
	}

	// Get object from OCI
	getResponse, err := c.objectStorageClient.GetObject(ctx, getRequest)
	if err != nil {
		var ociErr common.ServiceError
		if errors.As(err, &ociErr) {
			return nil, fmt.Errorf("failed to access object HttpStatusCode: %d\nOpcRequestId: %s\n message: %s\n ErrorCode: %s", ociErr.GetHTTPStatusCode(), ociErr.GetOpcRequestID(), ociErr.GetMessage(), ociErr.GetCode())

		}
		return nil, fmt.Errorf("failed to access object '%s' in bucket '%s': %w", c.path, c.bucketName, err)
	}
	defer getResponse.Content.Close() // ✅ Ensure response body is closed

	// Read object content
	contentArray, err := io.ReadAll(getResponse.Content)
	if err != nil {
		return nil, fmt.Errorf("unable to read 'content' from response: %w", err)
	}

	// Compute MD5 hash
	sum := md5.Sum(contentArray)
	md5Hash := base64.StdEncoding.EncodeToString(sum[:])

	// Construct payload
	payload := &remote.Payload{
		Data: contentArray,
		MD5:  []byte(md5Hash),
	}

	// Return an error instead of `nil, nil` if the object is empty
	if len(payload.Data) == 0 {
		return nil, fmt.Errorf("object %q is empty", c.path)
	}

	return payload, nil
}

func (c *RemoteClient) Put(data []byte) error {
	return c.putObject(data)
}

func (c *RemoteClient) putObject(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("putObject: data is empty")
	}

	ctx := context.Background()
	contentType := "application/json"
	sum := md5.Sum(data)

	putRequest := objectstorage.PutObjectRequest{
		ContentType:   common.String(contentType),
		NamespaceName: common.String(c.namespace),
		ObjectName:    common.String(c.path),
		BucketName:    common.String(c.bucketName),
		PutObjectBody: io.NopCloser(bytes.NewReader(data)),                      // ✅ Use NewReader instead of NewBuffer
		ContentMD5:    common.String(base64.StdEncoding.EncodeToString(sum[:])), // ✅ Fix MD5 encoding
	}

	// Handle encryption settings
	if c.kmsKeyID != "" {
		putRequest.OpcSseKmsKeyId = common.String(c.kmsKeyID)
	} else if c.serverSideEncryption && c.customerEncryptionKey != nil {
		if len(c.customerEncryptionKeySHA256) > 0 {
			putRequest.OpcSseCustomerKeySha256 = common.String(base64.StdEncoding.EncodeToString(c.customerEncryptionKeySHA256))
		} else {
			putRequest.OpcSseCustomerKey = common.String(base64.StdEncoding.EncodeToString(c.customerEncryptionKey))
			putRequest.OpcSseCustomerAlgorithm = common.String(encryptionAlgorithm)
		}
	}

	logger.Debug("Uploading remote state")

	putResponse, err := c.objectStorageClient.PutObject(ctx, putRequest)
	if err != nil {
		return fmt.Errorf("failed to upload object: %w", err)
	}

	logger.Debug("Uploaded statefile response: %+v\n", putResponse)
	return nil
}
func (c *RemoteClient) Delete() error {
	ctx := context.TODO()

	deleteRequest := objectstorage.DeleteObjectRequest{
		NamespaceName: common.String(c.namespace),
		ObjectName:    common.String(c.path),
		BucketName:    common.String(c.bucketName),
	}
	deleteResponse, err := c.objectStorageClient.DeleteObject(ctx, deleteRequest)
	if err != nil {
		return err
	}
	logger.Debug("delete statefile response: %+v\n", deleteResponse)
	return nil
}

func (c *RemoteClient) Lock(info *statemgr.LockInfo) (string, error) {
	ctx := context.TODO()
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return "", err
	}

	putObjReq := objectstorage.PutObjectRequest{
		BucketName:    common.String(c.bucketName),
		NamespaceName: common.String(c.namespace),
		ObjectName:    common.String(c.lockFilePath),
		IfNoneMatch:   common.String("*"),
		PutObjectBody: io.NopCloser(bytes.NewReader(infoBytes)),
	}

	putResponse, putErr := c.objectStorageClient.PutObject(ctx, putObjReq)
	if putErr != nil {
		return "", putErr
	}
	logger.Debug("state lock response code: %+d\n", putResponse.String())
	return info.ID, nil

}
func (c *RemoteClient) Unlock(id string) error {
	ctx := context.TODO()
	getRequest := objectstorage.GetObjectRequest{
		NamespaceName: common.String(c.namespace),
		ObjectName:    common.String(c.lockFilePath),
		BucketName:    common.String(c.bucketName),
	}
	getResponse, err := c.objectStorageClient.GetObject(ctx, getRequest)
	if err != nil {
		return err
	}
	lockByteData, err := io.ReadAll(getResponse.Content)
	if err != nil {
		return err
	}
	lockInfo := &statemgr.LockInfo{}
	if err := json.Unmarshal(lockByteData, lockInfo); err != nil {
		return fmt.Errorf("failed to unmarshal JSON data into LockInfo struct: %w", err)
	}
	// Verify that the provided lock ID matches the lock ID of the retrieved lock file.
	if lockInfo.ID != id {
		return fmt.Errorf("lock ID '%s' does not match the existing lock ID '%s'", id, lockInfo.ID)
	}

	deleteRequest := objectstorage.DeleteObjectRequest{
		NamespaceName: common.String(c.namespace),
		ObjectName:    common.String(c.lockFilePath),
		BucketName:    common.String(c.bucketName),
	}
	deleteResponse, err := c.objectStorageClient.DeleteObject(ctx, deleteRequest)
	if err != nil {
		return err
	}
	logger.Debug("Unlock response: %v\n", deleteResponse.String())
	return nil
}
