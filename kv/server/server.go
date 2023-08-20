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
	txn, regionErr := server.makeTxn(req.GetContext(), req.GetVersion())
	if regionErr != nil {
		response.RegionError = regionErr.RequestErr
		return response, nil
	}
	defer txn.Reader.Close()

	// 1. check if the key is locked
	if lock := getLock(txn, req.GetKey()); lock.IsLockedAt(req.GetVersion()) {
		// err1: the key has been locked
		response.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.GetKey()),
		}
		return response, nil
	}

	// 2. check if the value is valid
	if value := getValue(txn, req.GetKey()); value != nil {
		response.Value = value
	} else {
		// err2: the key is not found
		response.NotFound = true
	}
	return response, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	response := new(kvrpcpb.PrewriteResponse)
	txn, regionErr := server.makeTxn(req.GetContext(), req.GetStartVersion())
	if regionErr != nil {
		response.RegionError = regionErr.RequestErr
		return response, nil
	}
	defer txn.Reader.Close()

	for _, mutation := range req.GetMutations() {
		// 1. check if the key is locked
		if lock := getLock(txn, mutation.GetKey()); lock.IsLockedAt(req.GetStartVersion()) {
			// err1: the key has been locked
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(mutation.GetKey()),
			})
			log.Infof("KEY %+v is locked", mutation.GetKey())
			return response, nil
		}

		// 2. check if write conflict
		mostRecentWrite, commitTs, err := txn.MostRecentWrite(mutation.GetKey())
		if err != nil {
			log.Panic(err)
		}
		if mostRecentWrite != nil && mostRecentWrite.StartTS < req.GetStartVersion() && commitTs >= req.GetStartVersion() {
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
			return response, nil
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
		default:
			log.Panic("Err Wrong Mutation OP")
		}
	}

	// Fin. write to the storage
	server.storage.Write(req.GetContext(), txn.Writes())
	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	server.Latches.WaitForLatches(req.GetKeys())
	defer server.Latches.ReleaseLatches(req.GetKeys())

	response := new(kvrpcpb.CommitResponse)
	txn, regionErr := server.makeTxn(req.GetContext(), req.GetStartVersion())
	if regionErr != nil {
		response.RegionError = regionErr.RequestErr
		return response, nil
	}
	defer txn.Reader.Close()

	for _, key := range req.GetKeys() {
		write, _, err := txn.CurrentWrite(key)
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
			return response, nil
		}

		lock := getLock(txn, key)
		if !lock.IsLockedAt(req.GetStartVersion()) {
			log.Infof("Err Key %+v Unlocked", key)
			response.Error = &kvrpcpb.KeyError{
				Retryable: "Unlocked",
			}
			return response, nil
		}
		if lock.Ts != req.GetStartVersion() {
			log.Infof("Err Key %+v Locked By Another Transaction", key)
			response.Error = &kvrpcpb.KeyError{
				Retryable: "Locked By Another Txn",
			}
			return response, nil
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

	txn, regionErr := server.makeTxn(req.GetContext(), req.GetVersion())
	if regionErr != nil {
		response.RegionError = regionErr.RequestErr
		return response, nil
	}
	scanner := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scanner.Close()

	limit := req.GetLimit()
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
	txn, regionErr := server.makeTxn(req.GetContext(), req.GetLockTs())
	if regionErr != nil {
		response.RegionError = regionErr.RequestErr
		return response, nil
	}
	defer txn.Reader.Close()

	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		log.Panic(err)
	}

	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			response.CommitVersion = commitTs
		}
		return response, nil
	}

	lock := getLock(txn, req.GetPrimaryKey())
	if !lock.ExistAt(req.GetCurrentTs()) {
		response.Action = kvrpcpb.Action_LockNotExistRollback
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
	} else if lock.IsExpiredAt(req.GetCurrentTs()) {
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
	server.Latches.WaitForLatches(req.GetKeys())
	defer server.Latches.ReleaseLatches(req.GetKeys())

	response := new(kvrpcpb.BatchRollbackResponse)
	txn, regionErr := server.makeTxn(req.GetContext(), req.GetStartVersion())
	if regionErr != nil {
		response.RegionError = regionErr.RequestErr
		return response, nil
	}
	defer txn.Reader.Close()

	for _, key := range req.GetKeys() {
		write, _, err := txn.CurrentWrite(key)
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

		lock := getLock(txn, key)
		if lock.IsLockedBy(txn) {
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
	txn, regionErr := server.makeTxn(req.GetContext(), req.GetStartVersion())
	if regionErr != nil {
		response.RegionError = regionErr.RequestErr
		return response, nil
	}
	defer txn.Reader.Close()

	// traverse all locks
	keys := make([][]byte, 0)
	iter := txn.Reader.IterCF(engine_util.CfLock)
	defer iter.Close()

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

		if lock.IsLockedBy(txn) {
			keys = append(keys, key)
		}
		iter.Next()
	}

	if len(keys) == 0 {
		return response, nil
	}

	if req.GetCommitVersion() > 0 {
		commitResponse, err := server.KvCommit(context.TODO(), &kvrpcpb.CommitRequest{
			Context:       req.GetContext(),
			StartVersion:  req.GetStartVersion(),
			Keys:          keys,
			CommitVersion: req.GetCommitVersion(),
		})
		response.RegionError, response.Error = commitResponse.GetRegionError(), commitResponse.GetError()
		return response, err
	}
	// else
	rollbackResponse, err := server.KvBatchRollback(context.TODO(), &kvrpcpb.BatchRollbackRequest{
		Context:      req.GetContext(),
		StartVersion: req.GetStartVersion(),
		Keys:         keys,
	})

	response.RegionError, response.Error = rollbackResponse.GetRegionError(), rollbackResponse.GetError()
	return response, err
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

func getLock(txn *mvcc.MvccTxn, key []byte) *mvcc.Lock {
	lock, err := txn.GetLock(key)
	if err != nil {
		log.Panic(err)
	}
	return lock
}

func getValue(txn *mvcc.MvccTxn, key []byte) []byte {
	value, err := txn.GetValue(key)
	if err != nil {
		log.Panic(err)
	}
	return value
}

func (server *Server) makeTxn(ctx *kvrpcpb.Context, version uint64) (*mvcc.MvccTxn, *raft_storage.RegionError) {
	reader, err := server.storage.Reader(ctx)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return nil, regionErr
		}
		log.Panic(err)
	}
	return mvcc.NewMvccTxn(reader, version), nil
}
