package oci

import (
	"errors"
	"github.com/hashicorp/terraform/internal/backend"
	"github.com/hashicorp/terraform/internal/states/remote"
	"github.com/hashicorp/terraform/internal/states/statemgr"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

func (b *Backend) Workspaces() ([]string, error) {
	return nil, backend.ErrWorkspacesNotSupported
}
func (b *Backend) DeleteWorkspace(string, bool) error {
	return backend.ErrWorkspacesNotSupported
}
func (b *Backend) StateMgr(name string) (statemgr.Full, error) {
	if name != backend.DefaultStateName {
		return nil, backend.ErrWorkspacesNotSupported
	}
	remoteClient, err := b.remoteClient(name)
	if err != nil {
		return nil, err
	}
	return &remote.State{Client: remoteClient}, nil
}

func (b *Backend) remoteClient(name string) (*RemoteClient, error) {
	if name == "" {
		return nil, errors.New("missing state name")
	}
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(common.DefaultConfigProvider())
	if err != nil {
		return nil, err
	}
	return &RemoteClient{
		objectStorageClient: &client,
		bucketName:          b.Bucket,
		path:                b.path(name),
		namespace:           b.namespace,
		lockFilePath:        b.getLockFilePath(name),
	}, nil
}
