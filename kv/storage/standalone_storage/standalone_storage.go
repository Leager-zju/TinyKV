package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		conf: conf,
		db:   nil,
	}
}

func (s *StandAloneStorage) Start() error {
	db := engine_util.CreateDB(s.conf.DBPath, s.conf.Raft)
	if db == nil {
		log.Panic("Err Create DB Fail")
	}

	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			if err := engine_util.PutCF(s.db, modify.Cf(), modify.Key(), modify.Value()); err != nil {
				log.Panic(err)
			}
		case storage.Delete:
			if err := engine_util.DeleteCF(s.db, modify.Cf(), modify.Key()); err != nil {
				log.Panic(err)
			}
		default:
			log.Panic("Err Undefined Type")
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(r.txn, cf, key)
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	if err := r.txn.Commit(); err != nil {
		log.Panic(err)
	}
}
