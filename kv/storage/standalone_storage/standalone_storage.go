package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	Engine *engine_util.Engines
	Config *config.Config
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	kvEngine := engine_util.CreateDB("kv", conf);
	raftPath := filepath.Join(dbPath, "raft")
	raftEngine := engine_util.CreateDB("raft", conf)

	return &StandAloneStorage{
		Engine: engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath),
		Config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	//log.Info("call Start()")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	//log.Info("call Stop()")
	return s.Engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneReader{
		kvTxn: s.Engine.Kv.NewTransaction(false),
		raftTxn: s.Engine.Raft.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var txn *badger.Txn
	for _, m := range batch{
		switch m.Data.(type){
		case storage.Put:
			put := m.Data.(storage.Put)
			if put.Cf == "kv"{
				txn = s.Engine.Kv.NewTransaction(true)
			} else {
				txn = s.Engine.Raft.NewTransaction(true)
			}
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				return err
			}
			err = txn.Commit()
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			if delete.Cf == "kv"{
				txn = s.Engine.Kv.NewTransaction(true)
			} else {
				txn = s.Engine.Raft.NewTransaction(true)
			}
			err := txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key))
			if err != nil {
				return err
			}
			err = txn.Commit()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneReader struct {
	kvTxn   *badger.Txn
	raftTxn *badger.Txn
}

func (sreader *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	var txn *badger.Txn
	if cf == "raft" {
		txn = sreader.raftTxn
	} else {
		txn = sreader.kvTxn
	}

	val, err := engine_util.GetCFFromTxn(txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}



func (sreader *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	var txn *badger.Txn
	if cf == "raft" {
		txn = sreader.raftTxn
	} else {
		txn = sreader.kvTxn
	}
	return engine_util.NewCFIterator(cf, txn)
}



func (sreader *StandAloneReader) Close() {
	sreader.kvTxn.Discard()
	sreader.raftTxn.Discard()
}