// Package qiniu provides a storagedriver.StorageDriver implementation to
// store blobs in Qiniu cloud storage.
//
// This package leverages the official qiniu client library for interfacing with
// Qiniu.
//
// Because Qiniu is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// Keep in mind that Qiniu guarantees only read-after-write consistency for new
// objects, but no read-after-update or list-after-write consistency.
//
// +build include_qiniu

package qiniu

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	// "sort"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/getsentry/raven-go"
	"qiniupkg.com/api.v7/kodocli"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"qiniupkg.com/api.v7/kodo"
)

const driverName = "qiniu"

// minChunkSize defines the minimum multipart upload chunk size
// Qiniu API requires multipart upload chunks to be at least 2MB
const minChunkSize = 2 << 20

// expires after 3600 seconds
const urlExpires = 3600

const defaultChunkSize = 2 * minChunkSize

// listMax is the largest amount of objects you can request from Qiniu in a list call
const listMax = 1000

// validRegions maps known qiniu region identifiers to region descriptors
var validRegions = map[string]struct{}{}

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKey string
	SecretKey string

	Bucket string
	Domain string

	ChunkSize     int64
	RootDirectory string
	Transport     http.RoundTripper
	Zone          int
	// Region         string
	// RegionEndpoint string
	// Encrypt        bool
	// KeyID          string
	// Secure         bool
	// StorageClass   string
	// UserAgent      string

	RSHost  string
	RSFHost string
	IoHost  string
	UpHosts []string
}

func init() {
	// Register this driver
	factory.Register(driverName, &qiniuDriverFactory{})
}

// qiniuDriverFactory implements the factory.StorageDriverFactory interface
type qiniuDriverFactory struct{}

func (factory *qiniuDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Client    *kodo.Client
	Bucket    kodo.Bucket
	ChunkSize int64
	// Encrypt       bool
	// KeyID         string
	RootDirectory string
	// StorageClass  string
	Domain    string
	Zone      int
	UpHosts   []string
	Transport http.RoundTripper
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Qiniu
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - region
// - bucket
// - encrypt
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	// Providing no values for these is valid in case the user is authenticating
	// with an IAM on an ec2 instance (in which case the instance credentials will
	// be summoned when GetAuth is called)
	// accessKey := parameters["accesskey"]
	accessKey := "x"
	if accessKey == "" {
		accessKey = ""
	}
	// secretKey := parameters["secretkey"]
	secretKey := "xxx"
	if secretKey == "" {
		secretKey = ""
	}
	//
	// regionName, ok := parameters["region"]
	// if regionName == nil || fmt.Sprint(regionName) == "" {
	// 	return nil, fmt.Errorf("No region parameter provided")
	// }
	// region := fmt.Sprint(regionName)
	// // Don't check the region value if a custom endpoint is provided.
	// if regionEndpoint == "" {
	// 	if _, ok = validRegions[region]; !ok {
	// 		return nil, fmt.Errorf("Invalid region provided: %v", region)
	// 	}
	// }

	// bucket := parameters["bucket"]
	bucket := "test-docker"
	if bucket == "" || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}
	// domain := parameters["domain"]
	domain := "xxx"
	if domain == "" || fmt.Sprint(domain) == "" {
		return nil, fmt.Errorf("No domain parameter provided")
	}

	// encryptBool := false
	// encrypt := parameters["encrypt"]
	// switch encrypt := encrypt.(type) {
	// case string:
	// 	b, err := strconv.ParseBool(encrypt)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("The encrypt parameter should be a boolean")
	// 	}
	// 	encryptBool = b
	// case bool:
	// 	encryptBool = encrypt
	// case nil:
	// 	// do nothing
	// default:
	// 	return nil, fmt.Errorf("The encrypt parameter should be a boolean")
	// }
	//
	// secureBool := true
	// secure := parameters["secure"]
	// switch secure := secure.(type) {
	// case string:
	// 	b, err := strconv.ParseBool(secure)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("The secure parameter should be a boolean")
	// 	}
	// 	secureBool = b
	// case bool:
	// 	secureBool = secure
	// case nil:
	// 	// do nothing
	// default:
	// 	return nil, fmt.Errorf("The secure parameter should be a boolean")
	// }
	//
	// keyID := parameters["keyid"]
	// if keyID == nil {
	// 	keyID = ""
	// }

	chunkSize := int64(defaultChunkSize)
	chunkSizeParam := parameters["chunksize"]
	switch v := chunkSizeParam.(type) {
	case string:
		vv, err := strconv.ParseInt(v, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
		}
		chunkSize = vv
	case int64:
		chunkSize = v
	case int, uint, int32, uint32, uint64:
		chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
	case nil:
		// do nothing
	default:
		return nil, fmt.Errorf("invalid value for chunksize: %#v", chunkSizeParam)
	}

	if chunkSize < minChunkSize {
		return nil, fmt.Errorf("The chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
	}

	rootDirectory := parameters["rootdirectory"]
	if rootDirectory == nil {
		rootDirectory = ""
	}

	// storageClass := s3.StorageClassStandard
	// storageClassParam := parameters["storageclass"]
	// if storageClassParam != nil {
	// 	storageClassString, ok := storageClassParam.(string)
	// 	if !ok {
	// 		return nil, fmt.Errorf("The storageclass parameter must be one of %v, %v invalid", []string{s3.StorageClassStandard, s3.StorageClassReducedRedundancy}, storageClassParam)
	// 	}
	// 	// All valid storage class parameters are UPPERCASE, so be a bit more flexible here
	// 	storageClassString = strings.ToUpper(storageClassString)
	// 	if storageClassString != s3.StorageClassStandard && storageClassString != s3.StorageClassReducedRedundancy {
	// 		return nil, fmt.Errorf("The storageclass parameter must be one of %v, %v invalid", []string{s3.StorageClassStandard, s3.StorageClassReducedRedundancy}, storageClassParam)
	// 	}
	// 	storageClass = storageClassString
	// }
	//
	// userAgent := parameters["useragent"]
	// if userAgent == nil {
	// 	userAgent = ""
	// }
	//
	// encryptBool

	params := DriverParameters{
		AccessKey: fmt.Sprint(accessKey),
		SecretKey: fmt.Sprint(secretKey),
		Bucket:    fmt.Sprint(bucket),
		Domain:    domain,
		// regionEndpoint,
		// encryptBool,
		// fmt.Sprint(keyID),
		// secureBool,
		ChunkSize:     chunkSize,
		RootDirectory: fmt.Sprint(rootDirectory),
		Transport:     nil,
		RSHost:        "",
		RSFHost:       "",
		IoHost:        "",
		UpHosts:       []string{},
		Zone:          0,

		// storageClass,
		// fmt.Sprint(userAgent),
	}

	return New(params)
}

// New constructs a new Driver with the given AWS credentials, region, encryption flag, and
// bucketName
func New(params DriverParameters) (*Driver, error) {
	client := kodo.New(params.Zone, &kodo.Config{
		AccessKey: params.AccessKey,
		SecretKey: params.SecretKey,
		RSHost:    params.RSFHost,
		RSFHost:   params.RSFHost,
		IoHost:    params.IoHost,
		UpHosts:   params.UpHosts,
		Transport: params.Transport,
	})
	d := &driver{
		Client:        client,
		Bucket:        client.Bucket(params.Bucket),
		ChunkSize:     params.ChunkSize,
		RootDirectory: params.RootDirectory,
		Domain:        params.Domain,
		Zone:          params.Zone,
		UpHosts:       params.UpHosts,
		Transport:     params.Transport,
		// Bucket:        &client.Bucket(params.Bucket),
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

func (d *driver) downloadURL(key string) string {
	baseUrl := kodo.MakeBaseUrl(d.Domain, key)
	policy := kodo.GetPolicy{Expires: urlExpires}
	return d.Client.MakePrivateUrl(baseUrl, &policy)
}

func handleError(err error) {
	if err != nil {
		raven.CaptureError(err, nil)
	}
}

var notfoundError = errors.New("not found")

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	var err error
	defer func() {
		if err != nil {
			raven.CaptureError(err, nil)
		}
	}()
	reader, err := d.Reader(ctx, d.qnPath(path), 0)
	if err != nil {
		parseError(path, err)
		return nil, err
	}
	bys, err := ioutil.ReadAll(reader)
	parseError(path, err)
	log.Println(bys)
	return bys, err
}

type PutRet struct {
	Hash string `json:"hash"`
	Key  string `json:"key"`
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	key := d.qnPath(path)
	policy := d.newPutPolicy(key)
	//生成一个上传token
	token := d.newUptoken(policy)
	//构建一个uploader
	uploader := d.newUploader()

	var ret PutRet
	data := bytes.NewReader(contents)
	//调用PutFile方式上传，这里的key需要和上传指定的key一致
	err := uploader.Put(ctx, &ret, token, key, data, int64(len(contents)), nil)
	return err
}

func (d *driver) newUploader() (p kodocli.Uploader) {
	zone := d.Zone
	p = kodocli.NewUploader(zone, &kodocli.UploadConfig{
		UpHosts:   d.UpHosts,
		Transport: d.Transport,
	})
	return
}

func (d *driver) newPutPolicy(path string) *kodo.PutPolicy {
	policy := &kodo.PutPolicy{
		Scope: d.Bucket.Name + ":" + path,
		//设置Token过期时间
		Expires: urlExpires,
	}
	return policy
}

func (d *driver) newUptoken(policy *kodo.PutPolicy) string {
	return d.Client.MakeUptoken(policy)
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	var err error
	defer func() {
		if err != nil {
			raven.CaptureError(err, nil)
		}
	}()

	url := d.downloadURL(path)
	log.Println(url)
	client := http.Client{Transport: d.Transport}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, parseError(path, err)
	}
	req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	resp, err := client.Do(req)

	if err != nil {
		return nil, parseError(path, err)
	}
	if resp.StatusCode == 404 {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	return resp.Body, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	var err error
	defer func() {
		if err != nil {
			raven.CaptureError(err, nil)
		}
	}()

	key := d.qnPath(path)
	uploader := d.newUploader()
	if true {
		if err != nil {
			return nil, err
		}
		return d.newWriter(ctx, key, uploader, nil), nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	var err error
	defer func() {
		if err != nil {
			raven.CaptureError(err, nil)
		}
	}()

	entries, commonPrefixes, _, err := d.Bucket.List(ctx, d.qnPath(path), "", "", 2)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	fmt.Println(entries)
	if len(entries) == 1 {
		if entries[0].Key != d.qnPath(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = entries[0].Fsize
			fi.ModTime = time.Unix(entries[0].PutTime, 0)
		}
	} else if len(commonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	var err error
	defer func() {
		if err != nil {
			raven.CaptureError(err, nil)
		}
	}()

	path := opath
	if path != "/" && path[len(path)-1] != '/' {
		path = path + "/"
	}

	files := []string{}
	directories := []string{}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	prefix := ""
	if d.qnPath("") == "" {
		prefix = "/"
	}
	marker := ""
	limit := 1000
	entries := []kodo.ListItem{}
	// list all the objects
	entries, commonPrefixes, markerOut, err := d.Bucket.List(ctx, path, "/", marker, limit)
	log.Println(entries)
	marker = markerOut
	if err == io.EOF {
		return files, nil
	} else if err != nil {
		return nil, parseError(opath, err)
	}

	for {
		for _, item := range entries {
			files = append(files, strings.Replace(item.Key, d.qnPath(""), prefix, 1))
		}

		for _, commonPrefix := range commonPrefixes {
			// commonPrefix := *commonPrefix
			directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-1], d.qnPath(""), prefix, 1))
		}

		if err != io.EOF {
			entries, commonPrefixes, markerOut, err = d.Bucket.List(ctx, path, "/", marker, limit)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			// Treat empty response as missing directory, since we don't actually
			// have directories in s3.
			return nil, storagedriver.PathNotFoundError{Path: opath}
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	err := d.Bucket.Move(ctx, d.qnPath(sourcePath), d.qnPath(destPath))
	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
// We must be careful since S3 does not guarantee read after delete consistency
func (d *driver) Delete(ctx context.Context, path string) error {
	marker := ""
	limit := 1000
	entries := []kodo.ListItem{}
	var err error
	for {
		// list all the objects
		entries, _, marker, err = d.Bucket.List(ctx, d.qnPath(path), "", marker, limit)

		// resp.Contents can only be empty on the first call
		// if there were no more results to return after the first call, resp.IsTruncated would have been false
		// and the loop would be exited without recalling ListObjects
		if err != nil || len(entries) == 0 {
			return storagedriver.PathNotFoundError{Path: path}
		}

		for _, key := range entries {
			entries = append(entries, key)
		}

		// from the s3 api docs, IsTruncated "specifies whether (true) or not (false) all of the results were returned"
		// if everything has been returned, break
		if err == io.EOF {
			break
		}
	}

	// need to chunk objects into groups of 1000 per s3 restrictions
	total := len(entries)
	for i := 0; i < total; i += 1000 {
		keys := []string{}
		for _, item := range entries[i:min(i+1000, total)] {
			keys = append(keys, item.Key)
		}
		_, err := d.Bucket.BatchDelete(ctx, keys...)
		if err != nil {
			return err
		}
	}
	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	var err error
	defer func() {
		if err != nil {
			raven.CaptureError(err, nil)
		}
	}()

	baseUrl := kodo.MakeBaseUrl(d.Domain, d.qnPath(path))

	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != "GET" && methodString != "HEAD") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	// expiresIn := 20 * time.Minute
	// expires, ok := options["expiry"]
	// if ok {
	//		et, ok := expires.(time.Time)
	//	if ok {
	//		expiresIn = et.Sub(time.Now())
	//	}
	// }

	policy := kodo.GetPolicy{Expires: urlExpires}
	url := d.Client.MakePrivateUrl(baseUrl, &policy)

	switch methodString {
	case "GET", "HEAD":
		//
	default:
		panic("unreachable")
	}

	return url, nil
}

func (d *driver) qnPath(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.RootDirectory, "/")+path, "/")
}

// S3BucketKey returns the s3 bucket key for the given storage driver path.
func (d *Driver) QnBucketKey(path string) string {
	return d.StorageDriver.(*driver).qnPath(path)
}

func parseError(path string, err error) error {
	defer func() {
		if err != nil {
			raven.CaptureError(err, nil)
		}
	}()
	// if s3Err, ok := err.(awserr.Error); ok && s3Err.Code() == "NoSuchKey" {
	//		return storagedriver.PathNotFoundError{Path: path}
	// }
	log.Println(path)
	log.Println(err)
	return err
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

// writer attempts to upload parts to S3 in a buffered fashion where the last
// part is at least as large as the chunksize, so the multipart upload could be
// cleanly resumed in the future. This is violated if Close is called after less
// than a full chunk is written.
type writer struct {
	driver *driver
	ctx    context.Context
	ret    kodo.PutRet
	key    string
	// uploadID    string
	parts       []blockPart
	uploader    kodocli.Uploader
	size        int64
	readyPart   []byte
	pendingPart []byte
	closed      bool
	committed   bool
	cancelled   bool
}

type blockPart struct {
	ctx  string
	size int64
}

func (d *driver) newWriter(ctx context.Context, key string,
	uploader kodocli.Uploader, parts []blockPart) storagedriver.FileWriter {
	var size int64
	for _, part := range parts {
		size += part.size
	}
	return &writer{
		driver:   d,
		ctx:      ctx,
		key:      key,
		uploader: uploader,
		parts:    parts,
		size:     size,
	}
}

type completedParts []blockPart

func (a completedParts) Len() int      { return len(a) }
func (a completedParts) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// func (a completedParts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }

func (w *writer) Write(p []byte) (int, error) {
	var err error
	var n int
	defer func() {
		log.Println(len(p))
		log.Println(n)
		log.Println(w.size)
		if err != nil {
			raven.CaptureError(err, nil)
		}
	}()
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}
	fsize := int64(len(p))
	policy := w.driver.newPutPolicy(w.key)
	uptoken := w.driver.newUptoken(policy)
	f := bytes.NewReader(p)
	if fsize != 0 {
		err = rput(w.uploader, w.ctx, w.ret, uptoken, w.key, true, f, fsize, nil)
		if err != nil {
			n = 0
			return n, err
		}
	}
	// n = BlockCount(fsize)
	n = int(fsize)
	w.size += int64(n)
	return n, nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.flushPart()
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	var err error
	return err
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	err := w.flushPart()
	if err != nil {
		return err
	}
	w.committed = true
	err = mkfile(w.uploader, w.ctx, w.ret, w.key, true, w.size, nil)
	return err
}

// flushPart flushes buffers to write a part to S3.
// Only called by Write (with both buffers full) and Close/Commit (always)
func (w *writer) flushPart() error {
	return nil
}
