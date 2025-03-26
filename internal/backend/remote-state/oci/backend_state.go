package oci

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/terraform/internal/backend"
	"github.com/hashicorp/terraform/internal/states/remote"
	"github.com/hashicorp/terraform/internal/states/statemgr"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"strings"
)

const (
	keyEnvPrefix = "env:"
)

func (b *Backend) StateMgr(name string) (statemgr.Full, error) {
	if name != backend.DefaultStateName {
		return nil, backend.ErrWorkspacesNotSupported
	}
	err := b.configureRemoteClient(name)
	if err != nil {
		return nil, err
	}
	return &remote.State{Client: b.client}, nil
}

func (b *Backend) configureRemoteClient(name string) error {
	if name == "" {
		return errors.New("missing state name")
	}
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(common.DefaultConfigProvider())
	common.SetSDKLogger(logger)
	if err != nil {
		return err
	}
	b.client = &RemoteClient{
		objectStorageClient: &client,
		bucketName:          b.bucket,
		path:                b.path(name),
		namespace:           b.namespace,
		lockFilePath:        b.getLockFilePath(name),
	}
	return nil
}

func (b *Backend) Workspaces() ([]string, error) {
	const maxKeys = 1000

	ctx := context.TODO()
	prefix := b.key + keyEnvPrefix
	wss := []string{backend.DefaultStateName}
	start := common.String("")

	for {
		listObjectReq := objectstorage.ListObjectsRequest{
			BucketName:    common.String(b.bucket),
			NamespaceName: common.String(b.namespace),
			Prefix:        common.String(prefix),
			Start:         start,
			Limit:         common.Int(maxKeys),
		}
		listObjectResponse, err := b.client.objectStorageClient.ListObjects(ctx, listObjectReq)
		if err != nil {
			logger.Error("Failed to list workspaces in Object Storage backend: %v", err)
			return nil, err
		}

		for _, object := range listObjectResponse.Objects {
			key := *object.Name
			if strings.HasPrefix(key, prefix) {
				name := strings.TrimPrefix(key, prefix)
				// we store the state in a key, not a directory
				if strings.Contains(name, "/") {
					continue
				}

				wss = append(wss, name)
			}
		}
		if len(listObjectResponse.Objects) < maxKeys {
			break
		}
		start = listObjectResponse.NextStartWith

	}

	return wss, nil
}

func (b *Backend) DeleteWorkspace(name string, force bool) error {

	if name == backend.DefaultStateName || name == "" {
		return fmt.Errorf("can't delete default state")
	}

	logger.Info("Deleting workspace")

	return b.client.Delete()

}
