// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	// mysql package
	_ "github.com/go-sql-driver/mysql"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// mysql properties
const (
	mysqlHost       = "mysql.host"
	mysqlPort       = "mysql.port"
	mysqlUser       = "mysql.user"
	mysqlPassword   = "mysql.password"
	mysqlDBName     = "mysql.db"
	mysqlForceIndex = "mysql.force_index"
	mysqlTrans      = "mysql.transaction"
	mysqlTable      = "mysql.table"
	// TODO: support batch and auto commit
)

type Object struct {
	CustomAttributes map[string]string
	ACL              Acl
}

type Acl struct {
	CannedAcl string
}

type mysqlCreator struct {
}

type mysqlDB struct {
	p                 *properties.Properties
	db                *sql.DB
	verbose           bool
	forceIndexKeyword string
	trans             bool
	randomKey         bool
	table			  string

	bufPool *util.BufPool
}

func (d *mysqlDB) NewTrans() (tx *sql.Tx, err error) {
	tx, err = d.db.Begin()
	return
}

func (d *mysqlDB) AbortTrans(tx *sql.Tx) (err error) {
	err = tx.Rollback()
	return
}

func (d *mysqlDB) CommitTrans(tx *sql.Tx) (err error) {
	err = tx.Commit()
	return
}

type contextKey string

const stateKey = contextKey("mysqlDB")

type mysqlState struct {
	// Do we need a LRU cache here?
	stmtCache map[string]*sql.Stmt

	conn *sql.Conn
}

func (c mysqlCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(mysqlDB)
	d.p = p

	host := p.GetString(mysqlHost, "127.0.0.1")
	port := p.GetInt(mysqlPort, 3306)
	user := p.GetString(mysqlUser, "root")
	password := p.GetString(mysqlPassword, "")
	dbName := p.GetString(mysqlDBName, "test")
	dbTrans := p.GetString(mysqlTrans, "false")
	d.randomKey = p.GetBool(prop.RandomKey, false)
	d.table = p.GetString(mysqlTable, "hehe")
	rand.Seed(time.Now().UnixNano())
	if dbTrans == "false" {
		d.trans = false
	} else {
		d.trans = true
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbName)
	var err error
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	db.SetMaxIdleConns(threadCount + 1)
	db.SetMaxOpenConns(threadCount * 2)

	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	if p.GetBool(mysqlForceIndex, true) {
		d.forceIndexKeyword = "FORCE INDEX(`PRIMARY`)"
	}
	d.db = db

	d.bufPool = util.NewBufPool()

	if err := d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *mysqlDB) createTable() error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) && !db.p.GetBool(prop.DoTransactions, true) {
		if _, err := db.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			return err
		}
	}

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := db.p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (YCSB_KEY VARCHAR(64) PRIMARY KEY", tableName)
	buf.WriteString(s)

	for i := int64(0); i < fieldCount; i++ {
		buf.WriteString(fmt.Sprintf(", FIELD%d VARCHAR(%d)", i, fieldLength))
	}

	buf.WriteString(");")

	if db.verbose {
		fmt.Println(buf.String())
	}

	_, err := db.db.Exec(buf.String())
	return err
}

func (db *mysqlDB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

func (db *mysqlDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	conn, err := db.db.Conn(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to create db conn %v", err))
	}

	state := &mysqlState{
		stmtCache: make(map[string]*sql.Stmt),
		conn:      conn,
	}

	return context.WithValue(ctx, stateKey, state)
}

func (db *mysqlDB) CleanupThread(ctx context.Context) {
	state := ctx.Value(stateKey).(*mysqlState)

	for _, stmt := range state.stmtCache {
		stmt.Close()
	}
	state.conn.Close()
}

func (db *mysqlDB) getAndCacheStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	state := ctx.Value(stateKey).(*mysqlState)

	if stmt, ok := state.stmtCache[query]; ok {
		return stmt, nil
	}

	stmt, err := state.conn.PrepareContext(ctx, query)
	if err == sql.ErrConnDone {
		// Try build the connection and prepare again
		if state.conn, err = db.db.Conn(ctx); err == nil {
			stmt, err = state.conn.PrepareContext(ctx, query)
		}
	}

	if err != nil {
		return nil, err
	}

	state.stmtCache[query] = stmt
	return stmt, nil
}

func (db *mysqlDB) clearCacheIfFailed(ctx context.Context, query string, err error) {
	if err == nil {
		return
	}

	state := ctx.Value(stateKey).(*mysqlState)
	if stmt, ok := state.stmtCache[query]; ok {
		stmt.Close()
	}
	delete(state.stmtCache, query)
}

func (db *mysqlDB) queryRows(ctx context.Context, query string, count int, args ...interface{}) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	stmt, err := db.getAndCacheStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	vs := make([]map[string][]byte, 0, count)
	for rows.Next() {
		m := make(map[string][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			v := new([]byte)
			dest[i] = v
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}

		for i, v := range dest {
			m[cols[i]] = *v.(*[]byte)
		}

		vs = append(vs, m)
	}

	return vs, rows.Err()
}

func (db *mysqlDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s %s WHERE YCSB_KEY = ?`, table, db.forceIndexKeyword)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s %s WHERE YCSB_KEY = ?`, strings.Join(fields, ","), table, db.forceIndexKeyword)
	}

	rows, err := db.queryRows(ctx, query, 1, key)
	db.clearCacheIfFailed(ctx, query, err)

	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *mysqlDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s %s WHERE YCSB_KEY >= ? LIMIT ?`, table, db.forceIndexKeyword)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s %s WHERE YCSB_KEY >= ? LIMIT ?`, strings.Join(fields, ","), table, db.forceIndexKeyword)
	}

	rows, err := db.queryRows(ctx, query, count, startKey, count)
	db.clearCacheIfFailed(ctx, query, err)

	return rows, err
}

func (db *mysqlDB) execQuery(ctx context.Context, query string, args ...interface{}) error {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	if db.trans {
		tx, err := db.NewTrans()
		if err != nil {
			panic(err)
		}
		defer func() {
			if err != nil {
				db.AbortTrans(tx)
				panic(err)
			}
		}()

		_, err = tx.Exec(query, args...)
		if err != nil {
			panic(err)
		}
		err = db.CommitTrans(tx)
		if err != nil {
			panic(err)
		}
		return err
	} else {
		stmt, err := db.getAndCacheStmt(ctx, query)
		if err != nil {
			return err
		}

		_, err = stmt.ExecContext(ctx, args...)
		db.clearCacheIfFailed(ctx, query, err)
		return err
	}
}

func (db *mysqlDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf.WriteString("UPDATE ")
	buf.WriteString(table)
	buf.WriteString(" SET ")
	firstField := true
	pairs := util.NewFieldPairs(values)
	args := make([]interface{}, 0, len(values)+1)
	for _, p := range pairs {
		if firstField {
			firstField = false
		} else {
			buf.WriteString(", ")
		}

		buf.WriteString(p.Field)
		buf.WriteString(`= ?`)
		args = append(args, p.Value)
	}
	buf.WriteString(" WHERE YCSB_KEY = ?")

	args = append(args, key)

	return db.execQuery(ctx, buf.String(), args...)
}

func (db *mysqlDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	var ibucketname, iname, iversion, ilocation, ipool, iownerId, isize, iobjectId, ilastModifiedTime, ietag, icontentType, icustomattributes, iacl, ioullVersion, ideleteMarker, isseType, iencryptionKey, iinitializationVector, itype, istorageClass, value string
	args := make([]interface{}, 0, 1+len(values))
	if db.randomKey {
		value = key + "_" + strconv.FormatInt(rand.Int63(), 10)
	} else {
		value = key
	}
	args = append(args, value)

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	if db.table == "objects" { //If you specify a table name and the table name is objects, execute the method
		bucketName := "test_for_ycsb"
		name := strconv.FormatInt(time.Now().UnixNano(), 10) + "_" + strconv.FormatInt(rand.Int63(), 10)
		sqltext := "select bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag,contenttype," +
			"customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass from objects where bucketname=? and name=? order by bucketname,name,version limit 1"
		row := db.db.QueryRow(sqltext, bucketName, name)
		err = row.Scan(
			&ibucketname,
			&iname,
			&iversion,
			&ilocation,
			&ipool,
			&iownerId,
			&isize,
			&iobjectId,
			&ilastModifiedTime,
			&ietag,
			&icontentType,
			&icustomattributes,
			&iacl,
			&ioullVersion,
			&ideleteMarker,
			&isseType,
			&iencryptionKey,
			&iinitializationVector,
			&itype,
			&istorageClass,
		)
		buf.WriteString("INSERT IGNORE INTO ")
		buf.WriteString(table)
		buf.WriteString(" (bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag,contenttype,customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		v := math.MaxUint64 - uint64(time.Now().UnixNano())
		version := strconv.FormatUint(v, 10)
		location := "5f385d64-f877-4981-91a1-87befa409c84"
		pool := "tiger"
		ownerid := "3cebb65c-0a85-4b32-9f86-b527adee8f24"
		size := "20894192"
		objectid := "4347:1"
		lastmodifiedtime := "2019-11-19 01:59:54"
		etag := "f484d42635656c09fc9716e2d40201b1"
		contenttype := "application/x-sharedlib"
		o := new(Object)
		cus := make(map[string]string)
		cus["Content-Type"] = "application/x-sharedlib"
		cus["X-Amz-Meta-S3cmd-Attrs"] = "atime:1573803422/ctime:1573803423/gid:0/gname:root/md5:f484d42635656c09fc9716e2d40201b1/mode:33188/mtime:1573803423/uid:0/uname:root"
		cus["md5Sum"] = "12345678901234567890123456789012"
		o.CustomAttributes = cus
		o.ACL.CannedAcl = "private"
		customattributes, _ := json.Marshal(o.CustomAttributes)
		acl, _ := json.Marshal(o.ACL)
		nullversion := "1"
		deletemarker := "0"
		ssetype := ""
		encryptionkey := ""
		initializationvector := "NULL"
		typetype := "0"
		storageclass := "0"
		err = db.execQuery(ctx, buf.String(), bucketName, name, version, location, pool, ownerid, size, objectid, lastmodifiedtime, etag, contenttype, customattributes, acl, nullversion, deletemarker, ssetype, encryptionkey, initializationvector, typetype, storageclass)
	} else {
		buf.WriteString("INSERT IGNORE INTO ")
		buf.WriteString(table)
		buf.WriteString(" (YCSB_KEY")

		pairs := util.NewFieldPairs(values)
		for _, p := range pairs {
			args = append(args, p.Value)
			buf.WriteString(" ,")
			buf.WriteString(p.Field)
		}
		buf.WriteString(") VALUES (?")

		for i := 0; i < len(pairs); i++ {
			buf.WriteString(" ,?")
		}

		buf.WriteByte(')')
		err = db.execQuery(ctx, buf.String(), args...)
	}
	return err
}

func (db *mysqlDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE YCSB_KEY = ?`, table)

	return db.execQuery(ctx, query, key)
}

func (db *mysqlDB) Analyze(ctx context.Context, table string) error {
	_, err := db.db.Exec(fmt.Sprintf(`ANALYZE TABLE %s`, table))
	return err
}

func init() {
	ycsb.RegisterDBCreator("mysql", mysqlCreator{})
	ycsb.RegisterDBCreator("tidb", mysqlCreator{})
	ycsb.RegisterDBCreator("mariadb", mysqlCreator{})
}
