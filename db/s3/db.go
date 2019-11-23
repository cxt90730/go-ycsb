package s3

import (
	"bytes"
	"context"
	"github.com/dustin/go-humanize"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/credentials"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"io/ioutil"
	"math/rand"
	"sort"
	"strconv"
	"time"
)

const (
	endpoint        = "s3.endpoint"
	accessKey       = "s3.accessKeyId"
	secretKey       = "s3.secretKey"
	bucket          = "s3.bucket"
	useHttps        = "s3.useHttps"
	disableMd5Check = "s3.disableMd5"
	dataLength      = "s3.dataLength"
	onlyHead        = "s3.onlyHead"
)

type contextKey string

const stateKey = contextKey("s3Client")

type s3Creator struct{}

type s3Options struct {
	endpoint        string
	accessKey       string
	secretKey       string
	bucket          string
	useHttps        bool
	disableMd5Check bool
	dataLength      uint64
	validHead       bool
	randomKey       bool
}

type s3Client struct {
	p s3Options
}

type s3State struct {
	c *s3.S3
	b string
	d []byte
}

func (s s3Creator) Create(p *properties.Properties) (ycsb.DB, error) {
	return &s3Client{
		p: getOptions(p),
	}, nil
}

func getOptions(p *properties.Properties) s3Options {
	s3Endpoint := p.GetString(endpoint, "s3.test.com")
	s3AccessKey := p.GetString(accessKey, "hehehehe")
	s3SecretKey := p.GetString(secretKey, "hehehehe")
	s3Bucket := p.GetString(bucket, "hehe")
	s3UseHttps := p.GetBool(useHttps, false)
	s3DisableMd5 := p.GetBool(disableMd5Check, false)
	s3DataLength, err := humanize.ParseBytes(p.GetString(dataLength, "4KiB"))
	s3OnlyHead := p.GetBool(onlyHead, false)
	random := p.GetBool(prop.RandomKey, false)
	rand.Seed(time.Now().UnixNano())
	if err != nil {
		panic(err)
	}
	return s3Options{
		endpoint:        s3Endpoint,
		accessKey:       s3AccessKey,
		secretKey:       s3SecretKey,
		bucket:          s3Bucket,
		useHttps:        s3UseHttps,
		disableMd5Check: s3DisableMd5,
		dataLength:      s3DataLength,
		validHead:       s3OnlyHead,
		randomKey:       random,
	}
}

// Close closes the database layer.
func (c *s3Client) Close() error {
	return nil
}

func newS3(opts s3Options) *s3.S3 {
	creds := credentials.NewStaticCredentials(opts.accessKey, opts.secretKey, "")
	return s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials:                   creds,
			DisableSSL:                    aws.Bool(!opts.useHttps),
			Endpoint:                      aws.String(opts.endpoint),
			Region:                        aws.String("r"),
			S3DisableContentMD5Validation: aws.Bool(opts.disableMd5Check),
		},
	),
	),
	)
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
func (c *s3Client) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	mockData := make([]byte, c.p.dataLength)
	for i := 0; i < len(mockData); i++ {
		mockData[i] = uint8(i % 255)
	}
	client := newS3(c.p)
	state := &s3State{
		c: client,
		d: mockData,
		b: c.p.bucket,
	}
	return context.WithValue(ctx, stateKey, state)
}

// CleanupThread cleans up the state when the worker finished.
func (c *s3Client) CleanupThread(ctx context.Context) {

	return
}

// Read reads a record from the database and returns a map of each field/value pair.
// table: The name of the table.
// key: The record key of the record to read.
// fields: The list of fields to read, nil|empty for reading all.
func (c *s3Client) Read(ctx context.Context, table string, key string, fields []string) (result map[string][]byte, err error) {
	state := ctx.Value(stateKey).(*s3State)
	client := state.c
	if !c.p.validHead {
		input := &s3.GetObjectInput{
			Bucket: aws.String(state.b),
			Key:    aws.String(key),
		}
		object, err := client.GetObject(input)
		if err != nil {
			return nil, err
		}
		defer object.Body.Close()
		data, err := ioutil.ReadAll(object.Body)
		if err != nil {
			return nil, err
		}
		result := make(map[string][]byte)
		result[key] = data
	} else {
		input := &s3.HeadObjectInput{
			Bucket: aws.String(state.b),
			Key:    aws.String(key),
		}
		_, err := client.HeadObject(input)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Scan scans records from the database.
// table: The name of the table.
// startKey: The first record key to read.
// count: The number of records to read.
// fields: The list of fields to read, nil|empty for reading all.
func (c *s3Client) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var counter, startkeyNumber, numberOfIteration int
	counter = 0
	state := ctx.Value(stateKey).(*s3State)
	client := state.c
	params := &s3.ListObjectsInput{
		Bucket:    aws.String(state.b),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(1000),
	}
	list, err := client.ListObjects(params)
	if err != nil {
		return nil, err
	}
	var listSort []string
	for _, v := range list.Contents {
		listSort = append(listSort, *v.Key)
	}
	sort.Strings(listSort)
	for _, listWithSortKey := range listSort {
		if listWithSortKey == startKey {
			startkeyNumber = counter
		} else {
			counter = counter + 1
		}
	}
	if count < len(listSort) {
		numberOfIteration = count
	} else {
		numberOfIteration = len(listSort)
	}
	param := make(map[string][]byte)
	var result []map[string][]byte
	for i := startkeyNumber; i < numberOfIteration; i++ {
		key := listSort[i]
		input := &s3.GetObjectInput{
			Bucket: aws.String(state.b),
			Key:    aws.String(key),
		}
		object, err := client.GetObject(input)
		if err != nil {
			return nil, err
		}
		data, err := ioutil.ReadAll(object.Body)
		param[key] = data
		result = append(result, param)
	}
	return result, nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
// table: The name of the table.
// key: The record key of the record to update.
// values: A map of field/value pairs to update in the record.
func (c *s3Client) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	state := ctx.Value(stateKey).(*s3State)
	client := state.c
	params := &s3.GetObjectInput{
		Bucket: aws.String(state.b),
		Key:    aws.String(key),
	}
	out, err := client.GetObject(params)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(out.Body)
	input := &s3.PutObjectInput{
		Bucket:   aws.String(state.b),
		Key:      aws.String(key),
		Body:     bytes.NewReader(data),
		Metadata: out.Metadata,
	}
	_, err = client.PutObject(input)
	if err != nil {
		return err
	}
	return nil
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
// table: The name of the table.
// key: The record key of the record to insert.
// values: A map of field/value pairs to insert in the record.
func (c *s3Client) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	var value string
	if c.p.randomKey {
		value = key + "_" + strconv.FormatInt(rand.Int63(), 10)
	} else {
		value = key
	}
	state := ctx.Value(stateKey).(*s3State)
	client := state.c
	input := &s3.PutObjectInput{
		Bucket: aws.String(state.b),
		Key:    aws.String(value),
		Body:   bytes.NewReader(state.d),
	}
	_, err := client.PutObject(input)
	if err != nil {
		return err
	}
	return nil
}

// Delete deletes a record from the database.
// table: The name of the table.
// key: The record key of the record to delete.
func (c *s3Client) Delete(ctx context.Context, table string, key string) error {
	state := ctx.Value(stateKey).(*s3State)
	client := state.c
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(state.b),
		Key:    aws.String(key),
	}
	_, err := client.DeleteObject(input)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	ycsb.RegisterDBCreator("s3", s3Creator{})
}
