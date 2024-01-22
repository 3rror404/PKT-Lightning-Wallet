package sql

import (
	"database/sql"

	"github.com/pkt-cash/pktd/btcutil/er"
	"github.com/pkt-cash/pktd/pktwallet/walletdb"
	"github.com/pkt-cash/pktd/pktwallet/walletdb/sql/bucketpath"
	"github.com/pkt-cash/pktd/pktwallet/walletdb/sql/buckets"
)

type transaction_t struct {
	lastBucket *bucket_t
	sqltx      *sql.Tx
	onCommit   []func()
}

var _ walletdb.ReadTx = (*transaction_t)(nil)
var _ walletdb.ReadWriteTx = (*transaction_t)(nil)

func (tx *transaction_t) ReadBucket(key []byte) walletdb.ReadBucket {
	return tx.ReadWriteBucket(key)
}

func (tx *transaction_t) ReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	if key == nil {
		return tx.lastBucket
	}
	if !tx.hasBucket(key) {
		return nil
	}
	return bucketpath{id: key, txn: tx}
}

func (tx *transaction_t) CreateTopLevelBucket(key []byte) (walletdb.ReadWriteBucket, er.R) {
	return buckets.RootBucket(tx.sqltx, key, true)
}

func (tx *transaction_t) DeleteTopLevelBucket(key []byte) er.R {
	b := bucketpath.RootKey(key)
	_, err := tx.sqltx.Exec("DROP TABLE " + b.Table())
	return er.E(err)
}

func (tx *transaction_t) Commit() er.R {
	if tx.sqltx == nil {
		return er.Errorf("Cannot commit transaction, it has already been ended")
	}
	if err := tx.sqltx.Commit(); err != nil {
		return er.E(err)
	}
	tx.sqltx = nil
	for _, f := range tx.onCommit {
		f()
	}
	tx.onCommit = nil
	return nil
}

func (tx *transaction_t) Rollback() er.R {
	if tx.sqltx == nil {
		return er.Errorf("Cannot rollback transaction, it has already been ended")
	}
	if err := tx.sqltx.Rollback(); err != nil {
		return er.E(err)
	}
	tx.sqltx = nil
	return nil
}

func (tx *transaction_t) OnCommit(f func()) {
	tx.onCommit = append(tx.onCommit, f)
}

// func (tx *transaction_t) hasBucket(key *bucketpath.BucketPath) bool {
// 	// TODO
// 	return true
// }
