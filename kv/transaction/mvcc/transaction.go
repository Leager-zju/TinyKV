package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/log"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

/***************
 * TinyKV uses three column families (CFs):
 *  The "lock" CF: {the user key, a serialized Lock data structure}
 *  The "default" CF: {EncodeKey(user key, the start timestamp), the user value}
 *  The "write" CF: {EncodeKey(user key, the commit timestamp), a Write data structure}
 ***************/

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, curTs uint64, write *Write) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, curTs),
			Value: write.ToBytes(),
			Cf:    engine_util.CfWrite,
		},
	})
}

// GetLock returns a lock if key is locked.
// It will return (nil, nil) if there is no lock on key,
// and (nil, err) if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		log.Panic(err)
	}
	if value == nil {
		return nil, nil
	}

	lock, err := ParseLock(value)
	if err != nil {
		log.Panic(err)
	}
	if lock.Ts != txn.StartTS {
		lock = nil
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    engine_util.CfLock,
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  engine_util.CfLock,
		},
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	write, err := txn.LastCommitWrite(key)
	if err != nil {
		log.Panic(err)
	}
	if write == nil || write.Kind == WriteKindDelete {
		return nil, nil
	}

	startTS := write.StartTS
	// log.Infof("%+v", *write)
	readIter := txn.Reader.IterCF(engine_util.CfDefault)
	readIter.Seek(EncodeKey(key, startTS))
	if readIter.Valid() && bytes.Equal(DecodeUserKey(readIter.Item().Key()), key) {
		return readIter.Item().Value()
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS),
			Value: value,
			Cf:    engine_util.CfDefault,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: EncodeKey(key, txn.StartTS),
			Cf:  engine_util.CfDefault,
		},
	})
}

// LastCommitWrite searches for the latest commit WRITE before the transaction's start timestamp.
// It returns a Write from the DB, or an error.
func (txn *MvccTxn) LastCommitWrite(key []byte) (*Write, error) {
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	writeIter.Seek(EncodeKey(key, txn.StartTS))
	if !writeIter.Valid() {
		return nil, nil
	}

	// {EncodeKey(user key, the commit timestamp), a Write data structure}
	if !bytes.Equal(DecodeUserKey(writeIter.Item().Key()), key) {
		return nil, nil
	}

	value, err := writeIter.Item().Value()
	if err != nil {
		log.Panic(err)
	}

	write, err := ParseWrite(value)
	if err != nil {
		log.Panic(err)
	}

	return write, nil
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	writeIter := txn.Reader.IterCF(engine_util.CfWrite) // the latest commited WRITE
	for writeIter.Valid() {
		writeKey := writeIter.Item().Key() // {user key, the commit timestamp}
		value, err := writeIter.Item().Value()
		if err != nil {
			log.Panic(err)
		}

		write, err := ParseWrite(value)
		userKey, startTS, commitTS := DecodeUserKey(writeKey), write.StartTS, decodeTimestamp(writeKey)
		if err != nil {
			log.Panic(err)
		}

		if bytes.Equal(userKey, key) {
			if startTS < txn.StartTS {
				return nil, 0, nil
			}

			if startTS == txn.StartTS {
				return write, commitTS, nil
			}
		}
		writeIter.Next()
	}

	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	writeIter := txn.Reader.IterCF(engine_util.CfWrite) // the latest commited WRITE
	for writeIter.Valid() {
		writeKey := writeIter.Item().Key() // {user key, the commit timestamp}
		value, err := writeIter.Item().Value()
		if err != nil {
			log.Panic(err)
		}

		write, err := ParseWrite(value)
		userKey, commitTS := DecodeUserKey(writeKey), decodeTimestamp(writeKey)
		if err != nil {
			log.Panic(err)
		}

		if bytes.Equal(userKey, key) {
			return write, commitTS, nil
		}

		writeIter.Next()
	}

	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
