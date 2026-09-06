package ownership

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"os"
)

// Environment uses the same explicit S3 environment as the native object store.
// Credentials are never persisted in topology or evidence. Session tokens work;
// automatic instance-profile discovery is a separate deployment integration.
func Environment() (*TopologyStore, error) {
	bucket, prefix := os.Getenv("XENON_BUCKET"), os.Getenv("XENON_TOPOLOGY_PREFIX")
	return FromEnvironment(bucket, prefix)
}

// FromEnvironment keeps credentials external while taking cluster storage
// location from the unified agent configuration, without mutating process env.
func FromEnvironment(bucket, prefix string) (*TopologyStore, error) {
	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		return nil, fmt.Errorf("AWS_DEFAULT_REGION or AWS_REGION required")
	}
	key, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if key == "" || secret == "" {
		return nil, fmt.Errorf("explicit AWS credentials required")
	}
	cfg := aws.Config{Region: region, Credentials: credentials.NewStaticCredentialsProvider(key, secret, os.Getenv("AWS_SESSION_TOKEN"))}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	return NewTopologyStore(client, bucket, prefix)
}
