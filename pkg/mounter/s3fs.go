package mounter

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/s3"
)

// Implements Mounter
type s3fsMounter struct {
	meta      *s3.FSMeta
	url       string
	region    string
	accessKey string
	secretKey string
}

const (
	s3fsCmd = "s3fs"
)

func newS3fsMounter(meta *s3.FSMeta, cfg *s3.Config) (Mounter, error) {
	return &s3fsMounter{
		meta:      meta,
		url:       cfg.Endpoint,
		region:    cfg.Region,
		accessKey: cfg.AccessKeyID,
		secretKey: cfg.SecretAccessKey,
	}, nil
}

func (s3fs *s3fsMounter) Mount(target, volumeID string) error {
	if err := writeAWSCredentials(s3fs.accessKey, s3fs.secretKey); err != nil {
		return err
	}

	args := []string{
		fmt.Sprintf("%s:/%s", s3fs.meta.BucketName, s3fs.meta.Prefix),
		target,
		"-o", "use_path_request_style",
		"-o", fmt.Sprintf("url=%s", s3fs.url),
		"-o", "allow_other",
		"-o", "mp_umask=000",
	}
	if s3fs.region != "" {
		args = append(args, "-o", fmt.Sprintf("endpoint=%s", s3fs.region))
	}
	args = append(args, s3fs.meta.MountOptions...)
	return fuseMount(target, s3fsCmd, args, nil)
}

func writeAWSCredentials(accessKey, secretKey string) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %v", err)
	}

	awsDir := filepath.Join(homeDir, ".aws")
	if err := os.MkdirAll(awsDir, 0700); err != nil {
		return fmt.Errorf("failed to create .aws directory: %v", err)
	}

	credentialsPath := filepath.Join(awsDir, "credentials")
	credentials := fmt.Sprintf("[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n",
		accessKey, secretKey)

	if err := os.WriteFile(credentialsPath, []byte(credentials), 0600); err != nil {
		return fmt.Errorf("failed to write AWS credentials: %v", err)
	}

	return nil
}
