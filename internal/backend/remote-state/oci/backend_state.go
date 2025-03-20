package oci

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	baselogging "github.com/hashicorp/aws-sdk-go-base/v2/logging"
	"github.com/hashicorp/terraform/internal/backend"
	"sort"
)

func (b *Backend) Workspaces() ([]string, error) {
	const maxKeys = 1000

	ctx := context.TODO()
	log := logger()
	log = logWithOperation(log, operationBackendWorkspaces)
	log = log.With(
		logKeyBucket, b.bucketName,
	)

	prefix := ""

	if b.workspaceKeyPrefix != "" {
		prefix = b.workspaceKeyPrefix + "/"
	}

	log = log.With(
		logKeyBackendWorkspacePrefix, prefix,
	)

	params := &s3.ListObjectsV2Input{
		Bucket:  aws.String(b.bucketName),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(maxKeys),
	}

	wss := []string{backend.DefaultStateName}

	ctx, baselog := baselogging.NewHcLogger(ctx, log)
	ctx = baselogging.RegisterLogger(ctx, baselog)

	pages := s3.NewListObjectsV2Paginator(b.s3Client, params)
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			if IsA[*s3types.NoSuchBucket](err) {
				return nil, fmt.Errorf(errS3NoSuchBucket, b.bucketName, err)
			}
			if foo, ok := As[smithy.APIError](err); b.workspaceKeyPrefix == defaultWorkspaceKeyPrefix && ok && foo.ErrorCode() == "AccessDenied" {
				log.Warn("Unable to list non-default workspaces", "err", err.Error())
				return wss[:1], nil
			}
			return nil, fmt.Errorf("Unable to list objects in S3 bucket %q with prefix %q: %w", b.bucketName, prefix, err)
		}

		for _, obj := range page.Contents {
			ws := b.keyEnv(aws.ToString(obj.Key))
			if ws != "" {
				wss = append(wss, ws)
			}
		}
	}

	sort.Strings(wss[1:])
	return wss, nil
}
