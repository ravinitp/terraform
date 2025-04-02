package oci

import (
	"context"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/mock"
)

// MockObjectStorageClient is a mock of the ObjectStorageClient
type MockObjectStorageClient struct {
	mock.Mock
}

func (m *MockObjectStorageClient) CreateMultipartUpload(ctx context.Context, req objectstorage.CreateMultipartUploadRequest) (objectstorage.CreateMultipartUploadResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(objectstorage.CreateMultipartUploadResponse), args.Error(1)
}

func (m *MockObjectStorageClient) UploadPart(ctx context.Context, req objectstorage.UploadPartRequest) (objectstorage.UploadPartResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(objectstorage.UploadPartResponse), args.Error(1)
}

func (m *MockObjectStorageClient) CommitMultipartUpload(ctx context.Context, req objectstorage.CommitMultipartUploadRequest) (objectstorage.CommitMultipartUploadResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(objectstorage.CommitMultipartUploadResponse), args.Error(1)
}
