package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	lastKey []byte
	txn     *MvccTxn
	iter    engine_util.DBIterator
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

	encoded_key := scan.iter.Item().Key()
	decoded_key, startTs := DecodeUserKey(encoded_key), decodeTimestamp(encoded_key)
	value, err := scan.iter.Item().Value()
	if err != nil {
		return nil, nil, err
	}
	
	write, _, err := scan.txn.LastCommitWrite(decoded_key)
	if err != nil {
		return nil, nil, err
	}

	scan.iter.Next()
	// log.Infof("Key: %+v, Value: %+v, Start At: %+v, Write: %+v, Commit At: %d", key, value, startTs, write, commitTs)
	if bytes.Equal(scan.lastKey, decoded_key) {
		// log.Infof("has been collected before\n")
		return scan.Next()
	}
	if startTs > scan.txn.StartTS {
		// log.Infof("start in the future\n")
		return scan.Next()
	}
	if write == nil {
		// log.Infof("has not committed yet\n")
		return scan.Next()
	}
	if write.Kind != WriteKindPut {
		// log.Infof("has not been put\n")
		return scan.Next()
	}
	// log.Infof("collect success\n")
	scan.lastKey = decoded_key
	return decoded_key, value, err
}
