// Package objectstore puts and fetches bytes in an S3-compatible store, and hands
// out presigned URLs so clients can transfer them without the bytes passing through
// this process.
//
// Signature Version 4 by hand rather than through an SDK. Everything this system
// needs is four verbs against one bucket and a presigned URL, and the whole of the
// signing algorithm is the HMAC chain in signingKey below — where the AWS SDK is
// forty modules of configuration, credential providers and middleware for a
// deployment that has one endpoint and one static key. The check that it is right is
// a round trip against real MinIO in objectstore_test.go, which is a better guarantee
// than a dependency's reputation.
package objectstore

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"context"
)

// algorithm and unsignedPayload are fixed strings in the signing protocol.
const (
	algorithm = "AWS4-HMAC-SHA256"
	// unsignedPayload is what a presigned URL commits to instead of a body hash: the
	// signer does not have the bytes, and requiring it to would mean reading a
	// hundred megabytes into this process to authorise someone else sending them.
	unsignedPayload = "UNSIGNED-PAYLOAD"
)

// Config is where the bucket is and how to prove the right to write to it.
type Config struct {
	// Endpoint is the store's base URL, without a bucket — http://localhost:9000.
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
}

// Store is one bucket.
type Store struct {
	config Config
	base   *url.URL
	client *http.Client
	// now is the clock, injectable so a signature can be asserted against a known
	// timestamp rather than compared to itself.
	now func() time.Time
}

// New returns a store for the configured bucket.
func New(config Config, now func() time.Time) (*Store, error) {
	if config.Endpoint == "" || config.Bucket == "" || config.AccessKey == "" || config.SecretKey == "" {
		return nil, fmt.Errorf("objectstore: endpoint, bucket and credentials are all required")
	}
	base, err := url.Parse(config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("objectstore: parse endpoint: %w", err)
	}
	if config.Region == "" {
		config.Region = "us-east-1"
	}

	return &Store{
		config: config,
		base:   base,
		// A transfer of the original by the worker is the slowest thing here, and a
		// hundred megabytes over a slow link should not be cut off by a default.
		client: &http.Client{Timeout: 5 * time.Minute},
		now:    now,
	}, nil
}

// Presign returns a URL that authorises one request, and only that request.
//
// The headers given are signed, which is the point of the parameter: a URL signed
// with content-length is not usable to upload a different number of bytes, so the
// size limit is enforced by the store on a request this process never sees (MD-4).
func (s *Store) Presign(method, key string, headers map[string]string, expiry time.Duration) string {
	now := s.now().UTC()
	stamp := now.Format("20060102T150405Z")
	day := now.Format("20060102")
	scope := day + "/" + s.config.Region + "/s3/aws4_request"

	// host is always signed: without it a URL signed for one endpoint would be
	// replayable against another.
	signed := map[string]string{"host": s.base.Host}
	for name, value := range headers {
		signed[strings.ToLower(name)] = value
	}
	names := make([]string, 0, len(signed))
	for name := range signed {
		names = append(names, name)
	}
	sortStrings(names)

	var canonicalHeaders strings.Builder
	for _, name := range names {
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(strings.TrimSpace(signed[name]))
		canonicalHeaders.WriteString("\n")
	}
	signedHeaders := strings.Join(names, ";")

	query := url.Values{
		"X-Amz-Algorithm":     {algorithm},
		"X-Amz-Credential":    {s.config.AccessKey + "/" + scope},
		"X-Amz-Date":          {stamp},
		"X-Amz-Expires":       {strconv.Itoa(int(expiry.Seconds()))},
		"X-Amz-SignedHeaders": {signedHeaders},
	}
	// Encode sorts by key and percent-encodes, which is what the canonical form
	// requires. It would differ from the specification for a value containing a
	// space, which none of these do.
	canonicalQuery := query.Encode()

	path := s.path(key)
	canonicalRequest := strings.Join([]string{
		method, path, canonicalQuery, canonicalHeaders.String(), signedHeaders, unsignedPayload,
	}, "\n")

	toSign := strings.Join([]string{
		algorithm, stamp, scope, hex.EncodeToString(hash(canonicalRequest)),
	}, "\n")

	signature := hex.EncodeToString(mac(s.signingKey(day), toSign))
	return s.config.Endpoint + path + "?" + canonicalQuery + "&X-Amz-Signature=" + signature
}

// path is the canonical resource: path-style addressing, because a bucket in a
// hostname needs DNS entries a self-hosted store does not have.
func (s *Store) path(key string) string {
	path := "/" + s.config.Bucket
	for _, segment := range strings.Split(key, "/") {
		if segment == "" {
			continue
		}
		path += "/" + url.PathEscape(segment)
	}
	return path
}

// signingKey derives the day-, region- and service-scoped key.
//
// The chain is why a leaked signature is not a leaked credential: it can only sign
// for one day, one region and one service.
func (s *Store) signingKey(day string) []byte {
	key := mac([]byte("AWS4"+s.config.SecretKey), day)
	key = mac(key, s.config.Region)
	key = mac(key, "s3")
	return mac(key, "aws4_request")
}

func mac(key []byte, data string) []byte {
	hasher := hmac.New(sha256.New, key)
	hasher.Write([]byte(data))
	return hasher.Sum(nil)
}

func hash(data string) []byte {
	sum := sha256.Sum256([]byte(data))
	return sum[:]
}

// sortStrings avoids importing sort for one call on a slice of three.
func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

// signedExpiry is how long a presigned URL this process uses itself stays valid.
// Short, because these are used immediately.
const signedExpiry = 5 * time.Minute

// EnsureBucket creates the bucket if it is not there.
//
// Called at boot rather than left to a deployment step, so that a fresh checkout
// with a fresh MinIO works with no manual setup — the same reason migrations run
// automatically.
func (s *Store) EnsureBucket(ctx context.Context) error {
	response, err := s.do(ctx, http.MethodPut, "", nil, nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	// 409 is the bucket already existing and being ours, which is the normal case on
	// every boot after the first.
	if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusConflict {
		return nil
	}
	return failure("create bucket", response)
}

// Put writes an object.
func (s *Store) Put(ctx context.Context, key, contentType string, body []byte) error {
	headers := map[string]string{
		"content-type":   contentType,
		"content-length": strconv.Itoa(len(body)),
	}
	response, err := s.do(ctx, http.MethodPut, key, headers, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return failure("put "+key, response)
	}
	return nil
}

// Get reads an object whole.
//
// Whole rather than streamed: the caller is the worker producing variants, and
// decoding an image needs all of it. The size cap is what makes this bounded.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	response, err := s.do(ctx, http.MethodGet, key, nil, nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, failure("get "+key, response)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("objectstore: read %s: %w", key, err)
	}
	return body, nil
}

// Size reports how many bytes an object holds, and whether it is there at all.
//
// This is how a declared upload size is checked against what was actually
// transferred, without reading it.
func (s *Store) Size(ctx context.Context, key string) (int64, bool, error) {
	response, err := s.do(ctx, http.MethodHead, key, nil, nil)
	if err != nil {
		return 0, false, err
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusOK:
		return response.ContentLength, true, nil
	case http.StatusNotFound:
		return 0, false, nil
	default:
		return 0, false, failure("head "+key, response)
	}
}

// Delete removes an object. Absent is success: the caller wants it gone.
func (s *Store) Delete(ctx context.Context, key string) error {
	response, err := s.do(ctx, http.MethodDelete, key, nil, nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusNotFound {
		return nil
	}
	return failure("delete "+key, response)
}

// do signs a request and sends it. Every operation goes through a presigned URL,
// including this process's own, so there is one signing path to be correct.
func (s *Store) do(ctx context.Context, method, key string, headers map[string]string, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	request, err := http.NewRequestWithContext(ctx, method, s.Presign(method, key, headers, signedExpiry), reader)
	if err != nil {
		return nil, fmt.Errorf("objectstore: build request: %w", err)
	}
	for name, value := range headers {
		// Content-Length is a signed header and must reach the store, but net/http
		// takes it from the body rather than the header map — which a bytes.Reader
		// body already sets correctly.
		if strings.EqualFold(name, "content-length") {
			continue
		}
		request.Header.Set(name, value)
	}

	response, err := s.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("objectstore: %s %s: %w", method, key, err)
	}
	return response, nil
}

// failure turns a store error response into one this system can log.
//
// The body is included because S3 errors are XML documents whose code is the only
// way to tell "no such bucket" from "signature mismatch", and a bare status makes
// diagnosing a signing bug impossible.
func failure(what string, response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
	return fmt.Errorf("objectstore: %s: %s: %s", what, response.Status, strings.TrimSpace(string(body)))
}
