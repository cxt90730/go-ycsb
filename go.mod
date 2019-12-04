module github.com/pingcap/go-ycsb

require (
	cloud.google.com/go v0.38.0
	github.com/AndreasBriese/bbloom v0.0.0-20190825152654-46b345b51c96 // indirect
	github.com/XiaoMi/pegasus-go-client v0.0.0-20190415102652-337e0ea1d766
	github.com/aerospike/aerospike-client-go v2.3.0+incompatible
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/apache/thrift v0.13.0 // indirect
	github.com/apple/foundationdb/bindings/go v0.0.0-20191027010432-529b35a88625
	github.com/boltdb/bolt v1.3.1
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.17+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/dgraph-io/badger v1.5.4
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/facebookgo/ensure v0.0.0-20160127193407-b4ab57deab51 // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20150612182917-8dac2c3c4870 // indirect
	github.com/fortytw2/leaktest v1.3.0 // indirect

	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32 // indirect
	github.com/go-ini/ini v1.49.0 // indirect
	github.com/go-redis/redis v6.15.1+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/gocql/gocql v0.0.0-20181124151448-70385f88b28b
	github.com/gogo/protobuf v1.2.0 // indirect
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/google/pprof v0.0.0-20190908185732-236ed259b199 // indirect
	github.com/googleapis/gax-go v2.0.0+incompatible // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/journeymidnight/aws-sdk-go v1.18.1
	github.com/journeymidnight/radoshttpd v0.0.0-20190911073816-6df3523aa9e3
	github.com/kr/pty v1.1.8 // indirect
	github.com/lib/pq v0.0.0-20181016162627-9eb73efc1fcc
	github.com/magiconair/properties v1.8.0
	github.com/mattn/go-sqlite3 v1.10.0
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/minio/minio-go/v6 v6.0.39 // indirect
	github.com/openzipkin/zipkin-go v0.1.1 // indirect
	github.com/pingcap/errors v0.11.1
	github.com/pingcap/kvproto v0.0.0-20190506024016-26344dff8f48 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20190512091148-babf20351dd7 // indirect
	github.com/rogpeppe/go-internal v1.3.2 // indirect
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/tecbot/gorocksdb v0.0.0-20181010114359-8752a9433481
	github.com/tidwall/pretty v1.0.0 // indirect
	github.com/tikv/client-go v0.0.0-20190421092910-44b82dcc9f4a
	github.com/unrolled/render v1.0.1 // indirect
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/yuin/gopher-lua v0.0.0-20190514113301-1cd887cd7036 // indirect
	go.mongodb.org/mongo-driver v1.0.2
	go.opencensus.io v0.22.1 // indirect
	go.uber.org/multierr v1.2.0 // indirect
	go.uber.org/zap v1.11.0 // indirect
	google.golang.org/api v0.13.0
	google.golang.org/genproto v0.0.0-20190819201941-24fa4b261c55
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637 // indirect
	gopkg.in/yaml.v2 v2.2.4 // indirect
)

replace github.com/apache/thrift => github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d

replace github.com/tecbot/gorocksdb => github.com/DorianZheng/gorocksdb v0.0.0-20180811132858-ab9bf2cc4b67

go 1.13
