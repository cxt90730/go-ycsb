package rados

import (
	"context"
	"errors"
	"fmt"
	"github.com/journeymidnight/radoshttpd/rados"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"math/rand"
	"os"
	"time"
)

const (
	cephPath = "ceph.path"
	poolName = "ceph.pool"

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

const stateKey = contextKey("radosClient")

type radosCreator struct{}

type radosOptions struct {
	Path string
	Pool string
}

type radosClient struct {
	p          *properties.Properties
	conn       *rados.Conn
	pool       string
	fsid       string
	instanceId uint64
}

type radosState struct {
	// Do we need a LRU cache here?
	pool *rados.Pool
	oid  string
	data []byte
}

func (r radosCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := getOptions(p)
	fi, err := os.Stat(opts.Path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, errors.New("path can not be a directory")
	}
	d, err := os.Open(opts.Path)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	conn, err := rados.NewConn("admin")
	conn.SetConfigOption("rados_mon_op_timeout", MON_TIMEOUT)
	conn.SetConfigOption("rados_osd_op_timeout", OSD_TIMEOUT)
	err = conn.ReadConfigFile(opts.Path)
	if err != nil {
		return nil, err
	}

	err = conn.Connect()
	if err != nil {
		return nil, err
	}

	fsid, err := conn.GetFSID()
	if err != nil {
		conn.Shutdown()
		return nil, err
	}

	id := conn.GetInstanceID()
	c := &radosClient{
		p:          p,
		conn:       conn,
		fsid:       fsid,
		instanceId: id,
		pool: opts.Pool,
	}
	return c, nil
}

func getOptions(p *properties.Properties) radosOptions {
	path := p.GetString(cephPath, "/etc/ceph.conf")
	pool := p.GetString(poolName, "rabbit")
	return radosOptions{
		Path: path,
		Pool: pool,
	}
}

// Close closes the database layer.
func (r *radosClient) Close() error {
	r.conn.Shutdown()
	return nil
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
func (r *radosClient) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	pool, err := r.conn.OpenPool(r.pool)
	if err != nil {
		panic(err)
	}

	mockData4K := make([]byte, 4<<10)
	for i := 0; i < len(mockData4K); i++ {
		mockData4K[i] = uint8(i%255)
	}

	state := &radosState{
		pool: pool,
		data: mockData4K,
		oid : fmt.Sprintf("%d_%d_%d", r.instanceId, time.Now().UnixNano(), rand.Uint64()),
	}
	return context.WithValue(ctx, stateKey, state)
}

// CleanupThread cleans up the state when the worker finished.
func (r *radosClient) CleanupThread(ctx context.Context) {
	state := ctx.Value(stateKey).(*radosState)
	state.pool.Destroy()
	_ = state.data
	return
}

// Read reads a record from the database and returns a map of each field/value pair.
// table: The name of the table.
// key: The record key of the record to read.
// fields: The list of fields to read, nil|empty for reading all.
func (r *radosClient) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	return nil, nil
}

// Scan scans records from the database.
// table: The name of the table.
// startKey: The first record key to read.
// count: The number of records to read.
// fields: The list of fields to read, nil|empty for reading all.
func (r *radosClient) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
// table: The name of the table.
// key: The record key of the record to update.
// values: A map of field/value pairs to update in the record.
func (r *radosClient) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return nil
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
// table: The name of the table.
// key: The record key of the record to insert.
// values: A map of field/value pairs to insert in the record.
func (r *radosClient) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	state := ctx.Value(stateKey).(*radosState)
	return state.pool.WriteSmallObject(state.oid, state.data)
}

// Delete deletes a record from the database.
// table: The name of the table.
// key: The record key of the record to delete.
func (r *radosClient) Delete(ctx context.Context, table string, key string) error {
	return nil
}

func init() {
	ycsb.RegisterDBCreator("rados", radosCreator{})
}
