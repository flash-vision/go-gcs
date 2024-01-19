package gogcs

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

func GenerateUUID4() string {
	return uuid.New().String()
}

func getCloudObjectKey(url_path string) string {
	parsedurl, err := url.Parse(url_path)
	if err != nil {
		fmt.Printf("Error parsing url: %v", err)
		return ""
	}

	return strings.TrimPrefix(parsedurl.Path, "/")
}

func getCloudBucketFromUri(uri string) (bucket string, err error) {

	url, err := url.Parse(uri)
	if err != nil {
		return "", err
	}

	bucket = url.Host

	return bucket, nil
}

// GCPStorageClient is the interface for interacting with Google Cloud Storage
type GCPStorageClient interface {
	// GetBucket returns a handle to the specified bucket
	GetBucket(ctx context.Context, bucketName string) (*storage.BucketHandle, error)
	// GetObject returns a handle to the specified object
	GetObject(ctx context.Context, bucketName, objectName string) (*storage.ObjectHandle, error)
	// GetObjectIterator returns an iterator for listing objects in the specified bucket with the given prefix
	GetObjectIterator(ctx context.Context, bucketName, objectName string) *storage.ObjectIterator
	// SignedURL returns a signed URL for accessing the specified object in the specified bucket
	SignedURL(bucketName string, objectName string, expiration time.Duration) (string, error)
}

// gcpStorageClient is the implementation of the GCPStorageClient interface
type gcpStorageClient struct {
	client *storage.Client
}

// GetBucket returns a handle to the specified bucket
func (g *gcpStorageClient) GetBucket(ctx context.Context, bucketName string) (*storage.BucketHandle, error) {
	return g.client.Bucket(bucketName), nil
}

// GetObject returns a handle to the specified object
func (g *gcpStorageClient) GetObject(ctx context.Context, bucketName, objectName string) (*storage.ObjectHandle, error) {
	return g.client.Bucket(bucketName).Object(objectName), nil
}

// GetObjectIterator returns an iterator for listing objects in the specified bucket with the given prefix
func (g *gcpStorageClient) GetObjectIterator(ctx context.Context, bucketName, objectName string) *storage.ObjectIterator {
	return g.client.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: objectName})
}

// SignedURL returns a signed URL for accessing the specified object in the specified bucket
// SignedURL returns a signed URL for accessing the specified object in the specified bucket
func (g *gcpStorageClient) SignedURL(bucketName string, objectName string, expiration time.Duration) (string, error) {
	opts := &storage.SignedURLOptions{
		Method:  "GET", // Specify the HTTP method here
		Expires: time.Now().Add(expiration),
	}
	return g.client.Bucket(bucketName).SignedURL(objectName, opts)
}

// materialize a function for signed url
// SignedURL returns a signed URL for accessing the specified object in the specified bucket
func SignedURL(ctx context.Context, bucketName string, objectName string, expiration time.Duration) (string, error) {
	// create a new GCPStorageClient
	gcpStorageClient, err := NewGCPStorageClient(ctx, "")
	if err != nil {

		return "", err
	}
	return gcpStorageClient.SignedURL(bucketName, objectName, expiration)
}

// NewGCPStorageClient creates a new instance of the GCPStorageClient interface
func NewGCPStorageClient(ctx context.Context, projectID string) (GCPStorageClient, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &gcpStorageClient{client: client}, nil
}
