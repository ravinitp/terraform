// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package oci

import (
	"errors"
	"github.com/mitchellh/go-testing-interface"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMultiPartUploadImpl_Success(t *testing.T) {
	mockClient := new(MockObjectStorageClient)
	multipartData := MultipartUploadData{
		client: &RemoteClient{
			objectStorageClient: mockClient,
			namespace:           "test-namespace",
			bucketName:          "test-bucket",
			path:                "test-object",
		},
		Data:            []byte("test-data"),
		RequestMetadata: common.RequestMetadata{},
	}

	// Mock CreateMultipartUpload success response
	mockClient.On("CreateMultipartUpload", mock.Anything, mock.Anything).Return(
		objectstorage.CreateMultipartUploadResponse{
			MultipartUpload: objectstorage.MultipartUpload{UploadId: common.String("upload-id")},
		}, nil,
	)

	// Mock UploadPart success response
	mockClient.On("UploadPart", mock.Anything, mock.Anything).Return(
		objectstorage.UploadPartResponse{ETag: common.String("etag-123")}, nil,
	)

	// Mock CommitMultipartUpload success response
	mockClient.On("CommitMultipartUpload", mock.Anything, mock.Anything).Return(
		objectstorage.CommitMultipartUploadResponse{}, nil,
	)

	err := multipartData.multiPartUploadImpl()
	assert.NoError(t, err, "Expected no error in successful multipart upload")
}

func TestMultiPartUploadImpl_UploadFailure(t *testing.T) {
	mockClient := new(MockObjectStorageClient)
	multipartData := MultipartUploadData{
		client: &RemoteClient{
			objectStorageClient: mockClient,
			namespace:           "test-namespace",
			bucketName:          "test-bucket",
			path:                "test-object",
		},
		Data:            []byte("test-data"),
		RequestMetadata: common.RequestMetadata{},
	}

	// Mock CreateMultipartUpload success response
	mockClient.On("CreateMultipartUpload", mock.Anything, mock.Anything).Return(
		objectstorage.CreateMultipartUploadResponse{
			MultipartUpload: objectstorage.MultipartUpload{UploadId: common.String("upload-id")},
		}, nil,
	)

	// Mock UploadPart failure response
	mockClient.On("UploadPart", mock.Anything, mock.Anything).Return(
		objectstorage.UploadPartResponse{}, errors.New("upload part failed"),
	)

	err := multipartData.multiPartUploadImpl()
	assert.Error(t, err, "Expected an error in multipart upload failure")
	assert.Contains(t, err.Error(), "failed to upload part", "Error message should indicate part failure")
}
