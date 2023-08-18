package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/log"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	response := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Panic(err)
	}
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())

	// 1. check if locked
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		log.Panic(err)
	}
	if lock != nil && lock.Ts <= req.GetVersion() {
		// err1: the key has been locked
		response.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.GetKey()),
		}
		return response, nil
	}

	// 2. check if value is valid
	value, err := txn.GetValue(req.GetKey())
	if err != nil {
		log.Panic(err)
	}
	if value == nil {
		// err2: the key is not found
		response.NotFound = true
	} else {
		response.Value = value
	}
	return response, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	response := new(kvrpcpb.PrewriteResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Panic(err)
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	for _, mutation := range req.GetMutations() {
		// 1. check if locked
		lock, err := txn.GetLock(mutation.GetKey())
		if err != nil {
			log.Panic(err)
		}
		if lock != nil && lock.Ts <= req.GetStartVersion() {
			// err1: the key has been locked
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(mutation.GetKey()),
			})
			log.Infof("KEY %+v is locked", mutation.GetKey())
			continue
		}

		// 2. check if write conflict
		mostRecentWrite, commitTs, err := txn.MostRecentWrite(mutation.GetKey())
		if err != nil {
			log.Panic(err)
		}
		if mostRecentWrite != nil && mostRecentWrite.StartTS <= req.GetStartVersion() && commitTs >= req.GetStartVersion() {
			// err2: conflict with another transaction
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    mostRecentWrite.StartTS,
					ConflictTs: req.GetStartVersion(),
					Key:        mutation.GetKey(),
					Primary:    req.GetPrimaryLock(),
				},
			})
			log.Infof("the write to KEY %+v is conflict", mutation.GetKey())
			continue
		}

		// 3. lock the key
		txn.PutLock(mutation.GetKey(), &mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      req.GetStartVersion(),
			Ttl:     req.GetLockTtl(),
			Kind:    mvcc.WriteKind(mutation.GetOp() + 1),
		})

		// 4. collect the mutations
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.GetKey(), mutation.GetValue())
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.GetKey())
		case kvrpcpb.Op_Rollback:
			{
				// do nothing
			}
		default:
			log.Panic("Err Undefined Mutation OP")
		}
	}

	// Fin. write to the storage
	server.storage.Write(req.GetContext(), txn.Writes())
	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	response := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Panic(err)
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	for _, key := range req.GetKeys() {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			log.Panic(err)
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			log.Panic(err)
		}

		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				response.Error = &kvrpcpb.KeyError{
					Retryable: "Roll Back",
				}
				log.Infof("Err Key %+v Commit Roll Back", key)
			} else {
				log.Infof("Key %+v Committed Before", key)
			}
			break
		}

		if lock == nil {
			log.Infof("Err Key %+v Unlocked", key)
			response.Error = &kvrpcpb.KeyError{
				Retryable: "Unlocked",
			}
			break
		}

		if lock.Ts != req.GetStartVersion() {
			log.Infof("Err Key %+v Locked By Another Transaction", key)
			response.Error = &kvrpcpb.KeyError{
				Retryable: "Locked By Another Txn",
			}
			break
		}

		txn.DeleteLock(key)
		txn.PutWrite(key, req.GetCommitVersion(), &mvcc.Write{
			StartTS: req.GetStartVersion(),
			Kind:    lock.Kind,
		})
	}

	// Fin. write to the storage
	server.storage.Write(req.GetContext(), txn.Writes())
	return response, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	response := new(kvrpcpb.ScanResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Panic(err)
	}
	limit := req.GetLimit()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	scanner := mvcc.NewScanner(req.GetStartKey(), txn)

	for limit > 0 {
		key, value, err := scanner.Next()
		if err != nil {
			log.Panic(err)
		}

		limit--
		if key == nil || value == nil {
			break
		}

		response.Pairs = append(response.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}

	return response, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	response := new(kvrpcpb.CheckTxnStatusResponse) // default: NoAction
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Panic(err)
	}
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())

	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		log.Panic(err)
	}
	lock, err := txn.GetLock(req.GetPrimaryKey())
	if err != nil {
		log.Panic(err)
	}

	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			response.CommitVersion = commitTs
		}
		return response, nil
	}

	if lock == nil {
		response.Action = kvrpcpb.Action_LockNotExistRollback
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
	} else if lock.Ts+lock.Ttl < req.GetCurrentTs() {
		response.Action = kvrpcpb.Action_TTLExpireRollback
		txn.DeleteValue(req.GetPrimaryKey())
		txn.DeleteLock(req.GetPrimaryKey())
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
	}

	server.storage.Write(req.GetContext(), txn.Writes())
	return response, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	response := new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Panic(err)
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	for _, key := range req.GetKeys() {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			log.Panic(err)
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			log.Panic(err)
		}

		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				// has been rolled back before, no need to PutWrite again
				continue
			}
			response.Error = &kvrpcpb.KeyError{
				Abort: "Cannot rollback a committed write",
			}
			break
		}
		if lock != nil {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}

		txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
			StartTS: req.GetStartVersion(),
			Kind:    mvcc.WriteKindRollback,
		})
	}

	server.storage.Write(req.GetContext(), txn.Writes())
	return response, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	response := new(kvrpcpb.ResolveLockResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Panic(err)
	}

	commit := req.GetCommitVersion() != 0
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	// traverse all locks
	iter := reader.IterCF(engine_util.CfLock)
	for iter.Valid() {
		key := iter.Item().Key()
		value, err := iter.Item().Value()
		if err != nil {
			log.Panic(err)
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			log.Panic(err)
		}

		if lock.Ts == req.GetStartVersion() { // the same txn
			if commit {
				txn.DeleteLock(key)
				txn.PutWrite(key, req.GetCommitVersion(), &mvcc.Write{
					StartTS: req.GetStartVersion(),
					Kind:    lock.Kind,
				})
			} else {
				txn.DeleteValue(key)
				txn.DeleteLock(key)
				txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
					StartTS: req.GetStartVersion(),
					Kind:    mvcc.WriteKindRollback,
				})
			}
		}
		iter.Next()
	}

	server.storage.Write(req.GetContext(), txn.Writes())
	return response, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
