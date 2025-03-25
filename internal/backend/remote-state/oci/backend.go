// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package oci

import (
	"github.com/hashicorp/terraform/internal/backend"
	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/logging"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
	"path"
)

var (
	lockFileSuffix = ".lock"
	logger         = logging.NewLogger("tf-backend-oci")
)

func New() backend.Backend {
	return &Backend{}
}

// New creates a new backend for OSS remote state.
func (b *Backend) ConfigSchema() *configschema.Block {
	return &configschema.Block{
		Attributes: map[string]*configschema.Attribute{
			"key": {
				Type:        cty.String,
				Required:    true,
				Description: "The name of the state file stored on the remote backend.",
			},
			"bucket": {
				Type:        cty.String,
				Required:    true,
				Description: "The name of the OCI Object Storage bucket.",
			},
			"namespace": {
				Type:        cty.String,
				Required:    true,
				Description: "The namespace of the OCI Object Storage.",
			},
			"region": {
				Type:        cty.String,
				Optional:    true,
				Description: "OCI region where the bucket is located.",
			},
			"tenancy_ocid": {
				Type:        cty.String,
				Optional:    true,
				Description: "The OCID of the tenancy.",
			},
			"user_ocid": {
				Type:        cty.String,
				Optional:    true,
				Description: "The OCID of the user.",
			},
			"fingerprint": {
				Type:        cty.String,
				Optional:    true,
				Description: "The fingerprint of the user's API key.",
			},
			"private_key": {
				Type:        cty.String,
				Sensitive:   true,
				Optional:    true,
				Description: "The private key for API authentication.",
			},
			"private_key_path": {
				Type:        cty.String,
				Optional:    true,
				Description: "Path to the private key file.",
			},
			"private_key_password": {
				Type:        cty.String,
				Sensitive:   true,
				Optional:    true,
				Description: "Passphrase for the private key, if required.",
			},
			"auth_type": {
				Type:        cty.String,
				Optional:    true,
				Description: "Authentication method (API key, Instance Principal, Resource Principal, etc.).",
			},

			"config_file_profile": {
				Type:        cty.String,
				Optional:    true,
				Description: "Profile name from the OCI config file.",
			},
		},
	}
}

type Backend struct {
	bucket             string
	key                string
	namespace          string
	region             string
	tenancyOcid        string
	userOcid           string
	fingerprint        string
	privateKey         string
	privateKeyPath     string
	privateKeyPassword string
	authType           string
	configFileProfile  string
	workspaceKeyPrefix string
}

func (b *Backend) PrepareConfig(obj cty.Value) (cty.Value, tfdiags.Diagnostics) {
	return obj, tfdiags.Diagnostics{}
}
func (b *Backend) Configure(obj cty.Value) tfdiags.Diagnostics {
	var diags tfdiags.Diagnostics

	if obj.IsNull() {
		diags.Append(tfdiags.AttributeValue(tfdiags.Error, "Invalid Configuration", "Received null configuration for OCI backend.", cty.GetAttrPath(".")))
		return diags
	}

	if bucketVal := obj.GetAttr("bucket"); bucketVal.IsKnown() && !bucketVal.IsNull() {
		b.bucket = bucketVal.AsString()
	} else {
		diags.Append(tfdiags.AttributeValue(tfdiags.Error, "Missing Required Attribute", "Bucket name cannot be null", cty.GetAttrPath("bucket")))
	}
	if namespaceVal := obj.GetAttr("namespace"); namespaceVal.IsKnown() && !namespaceVal.IsNull() {
		b.namespace = namespaceVal.AsString()
	} else {
		diags.Append(tfdiags.AttributeValue(tfdiags.Error, "Missing Required Attribute", "Bucket name cannot be null", cty.GetAttrPath("namespace")))
	}
	if keyVal := obj.GetAttr("key"); keyVal.IsKnown() && !keyVal.IsNull() {
		b.key = keyVal.AsString()
	} else {
		diags.Append(tfdiags.AttributeValue(tfdiags.Error, "Missing Required Attribute", "The 'key' attribute must be specified.", cty.GetAttrPath("key")))
	}

	if regionVal := obj.GetAttr("region"); regionVal.IsKnown() && !regionVal.IsNull() {
		b.region = regionVal.AsString()
	}

	if tenancyOcidVal := obj.GetAttr("tenancy_ocid"); tenancyOcidVal.IsKnown() && !tenancyOcidVal.IsNull() {
		b.tenancyOcid = tenancyOcidVal.AsString()
	}

	if userOcidVal := obj.GetAttr("user_ocid"); userOcidVal.IsKnown() && !userOcidVal.IsNull() {
		b.userOcid = userOcidVal.AsString()
	}

	return diags
}

func (b *Backend) path(name string) string {
	if name == backend.DefaultStateName {
		return b.key
	}

	return path.Join(b.workspaceKeyPrefix, name, b.key)
}

// getLockFilePath returns the path to the lock file for the given Terraform state.
// For `default.tfstate`, the lock file is stored at `default.tfstate.tflock`.
func (b *Backend) getLockFilePath(name string) string {
	return b.path(name) + lockFileSuffix
}
