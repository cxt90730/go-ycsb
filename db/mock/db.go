package mock

import (
	"context"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"math/rand"
	"time"
	"net/http"
	"io/ioutil"
	"bytes"
	"github.com/dustin/go-humanize"
)

const (
	cephPath   = "ceph.path"
	poolName   = "ceph.pool"
	mockPort   = "mock.port"
	mockLength = "mock.dataLength"

	MON_TIMEOUT                = "10"
	OSD_TIMEOUT                = "10"
	STRIPE_UNIT                = 512 << 10 /* 512K */
	STRIPE_COUNT               = 2
	OBJECT_SIZE                = 8 << 20 /* 8M */
	AIO_CONCURRENT             = 4
	DEFAULT_CEPHCONFIG_PATTERN = "conf/*.conf"
	MIN_CHUNK_SIZE             = 512 << 10       // 512K
	BUFFER_SIZE                = 1 << 20         // 1M
	MAX_CHUNK_SIZE             = 8 * BUFFER_SIZE // 8M
)

type contextKey string

const stateKey = contextKey("mockClient")

type mockCreator struct{}

type mockOptions struct {
	Path       string
	Pool       string
	Port       string
	DataLength uint64
}

type mockClient struct {
	p    *properties.Properties
	l    uint64
	path string
	pool string
}

type mockState struct {
	// Do we need a LRU cache here?
	pool string
	oid  string
	data []byte
}

func (r mockCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := getOptions(p)
	c := &mockClient{
		p:    p,
		l:    opts.DataLength,
		path: opts.Path,
		pool: opts.Pool,
	}
	newMockServer(opts.Path, opts.Port)
	return c, nil
}

func getOptions(p *properties.Properties) mockOptions {
	path := p.GetString(cephPath, "/etc/ceph.conf")
	pool := p.GetString(poolName, "rabbit")
	port := p.GetString(mockPort, "80")
	length, err := humanize.ParseBytes(p.GetString(mockLength, "4KiB"))
	if err != nil {
		panic(err)
	}
	return mockOptions{
		Path:       path,
		Pool:       pool,
		Port:       port,
		DataLength: length,
	}
}

func newMockServer(path string, port string) {
	conn, err := newCephConn(path)
	if err != nil {
		panic(err)
	}
	s := &http.Server{
		Addr:           ":" + port,
		ReadTimeout:    10 * time.Minute,
		WriteTimeout:   10 * time.Minute,
		MaxHeaderBytes: 1 << 20,
		Handler:        &myHandler{conn},
	}
	go func() {
		err := s.ListenAndServe()
		panic(err)
	}()
}

// Close closes the database layer.
func (r *mockClient) Close() error {
	return nil
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
func (r *mockClient) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	mockData4K := make([]byte, r.l)
	for i := 0; i < len(mockData4K); i++ {
		mockData4K[i] = uint8(i % 255)
	}
	state := &mockState{
		pool: r.pool,
		data: mockData4K,
		oid:  fmt.Sprintf("%d_%d", time.Now().UnixNano(), rand.Uint64()),
	}
	return context.WithValue(ctx, stateKey, state)
}

// CleanupThread cleans up the state when the worker finished.
func (r *mockClient) CleanupThread(ctx context.Context) {
	state := ctx.Value(stateKey).(*mockState)
	_ = state.data
	return
}

// Read reads a record from the database and returns a map of each field/value pair.
// table: The name of the table.
// key: The record key of the record to read.
// fields: The list of fields to read, nil|empty for reading all.
func (r *mockClient) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	return nil, nil
}

// Scan scans records from the database.
// table: The name of the table.
// startKey: The first record key to read.
// count: The number of records to read.
// fields: The list of fields to read, nil|empty for reading all.
func (r *mockClient) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
// table: The name of the table.
// key: The record key of the record to update.
// values: A map of field/value pairs to update in the record.
func (r *mockClient) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return nil
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
// table: The name of the table.
// key: The record key of the record to insert.
// values: A map of field/value pairs to insert in the record.
func (r *mockClient) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	state := ctx.Value(stateKey).(*mockState)
	req, err := http.NewRequest("PUT", "localhost", bytes.NewReader(state.data))
	if err != nil {
		return err
	}
	req.Header.Set("Pool", state.pool)
	req.Header.Set("Oid", state.oid)
	c := http.DefaultClient
	res, err := c.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		d, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		fmt.Println(string(d))
	}
	return nil
}

// Delete deletes a record from the database.
// table: The name of the table.
// key: The record key of the record to delete.
func (r *mockClient) Delete(ctx context.Context, table string, key string) error {
	return nil
}

func init() {
	ycsb.RegisterDBCreator("mock", mockCreator{})
}
