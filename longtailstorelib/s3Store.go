package longtailstorelib

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type S3Options struct {
	EndpointResolverURI string
	Anonymous           bool
	CannedACL           string
}

type s3BlobStore struct {
	bucketName string
	prefix     string
	options    S3Options
}

type s3BlobClient struct {
	ctx    context.Context
	store  *s3BlobStore
	client *s3.Client
}

type s3BlobObject struct {
	ctx    context.Context
	client *s3BlobClient
	path   string
}

// NewS3BlobStore ...
func NewS3BlobStore(u *url.URL, opts ...BlobStoreOption) (BlobStore, error) {
	const fname = "NewS3BlobStore"
	if u.Scheme != "s3" {
		err := fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
		return nil, errors.Wrap(err, fname)
	}
	prefix := u.Path
	if len(u.Path) > 0 {
		prefix = u.Path[1:] // strip initial slash
	}

	if prefix != "" {
		prefix += "/"
	}
	s := &s3BlobStore{bucketName: u.Host, prefix: prefix}
	for _, opt := range opts {
		opt(&s.options)
	}
	return s, nil
}

func (blobStore *s3BlobStore) NewClient(ctx context.Context) (BlobClient, error) {
	const fname = "s3BlobStore.NewClient"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	if blobStore.options.Anonymous {
		log.Error("Using anon creds")
		cfg.Credentials = aws.AnonymousCredentials{}
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if blobStore.options.EndpointResolverURI != "" {
			o.EndpointResolver = s3.EndpointResolverFromURL(blobStore.options.EndpointResolverURI)
		}
	})
	return &s3BlobClient{store: blobStore, ctx: ctx, client: client}, nil
}

func (blobStore *s3BlobStore) String() string {
	return "s3://" + blobStore.bucketName + "/" + blobStore.prefix
}

func (blobStore *s3BlobStore) Options() BlobStoreOption {
	return func(options interface{}) {
		s3options, ok := options.(*S3Options)
		if !ok {
			return
		}
		s3options.EndpointResolverURI = blobStore.options.EndpointResolverURI
		s3options.Anonymous = blobStore.options.Anonymous
		s3options.CannedACL = blobStore.options.CannedACL
	}
}

func (blobClient *s3BlobClient) NewObject(path string) (BlobObject, error) {
	s3Path := blobClient.store.prefix + path
	return &s3BlobObject{
			ctx:    blobClient.ctx,
			client: blobClient,
			path:   s3Path},
		nil
}

func (blobClient *s3BlobClient) GetObjects(pathPrefix string) ([]BlobProperties, error) {
	const fname = "s3BlobClient.GetObjects"
	var items []BlobProperties
	output, err := blobClient.client.ListObjectsV2(blobClient.ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(blobClient.store.bucketName),
		Prefix: aws.String(blobClient.store.prefix + pathPrefix),
	})
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	for _, object := range output.Contents {
		itemName := aws.ToString(object.Key)[len(blobClient.store.prefix):]
		items = append(items, BlobProperties{Size: *object.Size, Name: itemName})
	}
	return items, nil
}

func (blobClient *s3BlobClient) SupportsLocking() bool {
	return false
}

func (blobClient *s3BlobClient) Close() {
	blobClient.client = nil
}

func (blobClient *s3BlobClient) String() string {
	return blobClient.store.String()
}

func (blobObject *s3BlobObject) Read() ([]byte, error) {
	const fname = "s3BlobObject.Read()"
	input := &s3.GetObjectInput{
		Bucket: aws.String(blobObject.client.store.bucketName),
		Key:    aws.String(blobObject.path),
	}
	result, err := blobObject.client.client.GetObject(blobObject.client.ctx, input)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			err = errors.Wrapf(os.ErrNotExist, "%v", err)
			return nil, errors.Wrap(err, fname)
		}
		return nil, err
	}
	data, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	result.Body.Close()
	return data, nil
}

func (blobObject *s3BlobObject) LockWriteVersion() (bool, error) {
	return false, nil
}

func (blobObject *s3BlobObject) Exists() (bool, error) {
	const fname = "s3BlobObject.Exists()"
	input := &s3.HeadObjectInput{
		Bucket: aws.String(blobObject.client.store.bucketName),
		Key:    aws.String(blobObject.path),
	}
	_, err := blobObject.client.client.HeadObject(blobObject.client.ctx, input)
	if err != nil {
		str := err.Error()
		if strings.Contains(str, "StatusCode: 404") {
			return false, nil
		} else {
			return false, errors.Wrap(err, fname)
		}
	}
	return true, nil
}

func (blobObject *s3BlobObject) Write(data []byte) (bool, error) {
	const fname = "s3BlobObject.Write()"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	input := &s3.PutObjectInput{
		Bucket: aws.String(blobObject.client.store.bucketName),
		Key:    aws.String(blobObject.path),
		Body:   bytes.NewReader(data),
	}
	if blobObject.client.store.options.CannedACL != "" {
		log.Errorf("Setting ACL to %s", blobObject.client.store.options.CannedACL)
		input.ACL = types.ObjectCannedACL(blobObject.client.store.options.CannedACL)
	}
	_, err := blobObject.client.client.PutObject(blobObject.client.ctx, input)
	if err != nil {
		return true, errors.Wrap(err, fname)
	}
	return true, nil
}

func (blobObject *s3BlobObject) Delete() error {
	const fname = "s3BlobObject.Delete()"
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(blobObject.client.store.bucketName),
		Key:    aws.String(blobObject.path),
	}
	_, err := blobObject.client.client.DeleteObject(blobObject.client.ctx, input)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

func (blobObject *s3BlobObject) String() string {
	return fmt.Sprintf("%s/%s", blobObject.client.String(), blobObject.path)
}
