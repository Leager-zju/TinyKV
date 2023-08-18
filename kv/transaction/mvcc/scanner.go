package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	scanner := &Scanner{
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfDefault),
	}
	scanner.iter.Seek(startKey)
	return scanner
}

func (scan *Scanner) Close() {
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	defer scan.iter.Next()

	encoded_key := scan.iter.Item().Key()
	key, _ := DecodeUserKey(encoded_key), decodeTimestamp(encoded_key)
	value, err := scan.iter.Item().Value()
	if err != nil {
		return nil, nil, err
	}
	write, commitTs, err := scan.txn.MostRecentWrite(key)
	if err != nil {
		return nil, nil, err
	}
	log.Infof("Key: %+v, Value: %+v, Write: %+v, Commit At: %d", key, value, write, commitTs)

	if write == nil || commitTs > scan.txn.StartTS { // has not committed yet
		return scan.Next()
	}
	if write.Kind == WriteKindRollback { // has been rolled back
		return scan.Next()
	}

	return key, value, err
}
