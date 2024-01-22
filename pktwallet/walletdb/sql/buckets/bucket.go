package buckets

import (
	"database/sql"
	"fmt"

	"github.com/pkt-cash/pktd/btcutil/er"
	"github.com/pkt-cash/pktd/pktlog/log"
	"github.com/pkt-cash/pktd/pktwallet/walletdb"
	"go.etcd.io/bbolt"
)

type Bucket struct {
	//	path  [][]byte
	id    bucketId
	table string
	sqltx *sql.Tx
}

// Enforce bucket implements the walletdb Bucket interfaces.
var _ walletdb.ReadWriteBucket = (*Bucket)(nil)
var _ walletdb.ReadBucket = (*Bucket)(nil)

// Child buckets

func (b *Bucket) NestedReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	childPath := b.id.withAppended(key)
	child, err := mkBucket(b.sqltx, childPath, false)
	if err != nil {
		log.Warnf("Unable to open bucket [%s] because [%s]", childPath, err)
		return nil
	}
	return child
}

func (b *Bucket) NestedReadBucket(key []byte) walletdb.ReadBucket {
	return b.NestedReadWriteBucket(key)
}

func (b *Bucket) CreateBucket(key []byte) (walletdb.ReadWriteBucket, er.R) {
	childPath := b.id.withAppended(key)
	child, _ := mkBucket(b.sqltx, childPath, false)
	if child != nil {
		return nil, er.Errorf("Cannot create bucket [%s], already exists", childPath)
	}
	child, err := mkBucket(b.sqltx, childPath, true)
	if err != nil {
		err.AddMessage(fmt.Sprintf("Unable to create bucket [%s]", childPath))
		return nil, err
	}
	return child, nil
}

func (b *Bucket) CreateBucketIfNotExists(key []byte) (walletdb.ReadWriteBucket, er.R) {
	childPath := b.id.withAppended(key)
	child, err := mkBucket(b.sqltx, childPath, true)
	if err != nil {
		err.AddMessage(fmt.Sprintf("Unable to create bucket [%s]", childPath))
		return nil, err
	}
	return child, nil
}

func (b *Bucket) DeleteNestedBucket(key []byte) er.R {
	childPath := b.id.withAppended(key)
	return destroyBucket(b.sqltx, &childPath)
}

//// Sequences

func (b *Bucket) NextSequence() (uint64, er.R) {
	res, err := b.sqltx.Query(
		"UPDATE bucketindex SET sequence = sequence + 1 WHERE path = ? RETURNING sequence", b.id.bytes)
	if err != nil {
		return 0, er.E(err)
	}
	var seq uint64
	if err := res.Scan(&seq); err != nil {
		return 0, er.E(err)
	}
	return seq, nil
}

func (b *Bucket) SetSequence(v uint64) er.R {
	_, err := b.sqltx.Exec("UPDATE bucketindex SET sequence = ? WHERE path = ?", b.id.bytes, v)
	if err != nil {
		return er.Errorf("Error setting sequence in [%s]: [%s]", &b.id, err)
	}
	return nil
}

func (b *Bucket) Sequence() uint64 {
	res, err := b.sqltx.Query("SELECT sequence FROM bucketindex WHERE path = ?", b.id.bytes)
	if err != nil {
		log.Errorf("Error getting sequence (Query()) in [%s]: [%s]", &b.id, err)
		return 0
	}
	var seq uint64
	if err := res.Scan(&seq); err != nil {
		log.Errorf("Error getting sequence (Scan()) in [%s]: [%s]", &b.id, err)
		return 0
	}
	return seq
}

////// Get/Put/Delete

func (b *Bucket) Put(key, value []byte) er.R {
	_, err := b.sqltx.Exec("INSERT INTO "+b.table+"(key,val) VALUES(?,?) "+
		"ON CONFLICT(key) DO UPDATE SET value=?", key, value, value)
	if err != nil {
		return er.Errorf("Error inserting in [%s]: [%s]", &b.id, err)
	}
	return nil
}

func (b *Bucket) Get(key []byte) []byte {
	res, err := b.sqltx.Query("SELECT value FROM "+b.table+" WHERE key = ?", key)
	if err != nil {
		log.Errorf("Error getting value (Query()) in [%s]: [%s]", &b.id, err)
		return nil
	}
	var v []byte
	if err := res.Scan(&v); err != nil {
		log.Errorf("Error getting value (Scan()) in [%s]: [%s]", &b.id, err)
		return nil
	}
	return v
}

func (b *Bucket) Delete(key []byte) er.R {
	_, err := b.sqltx.Exec("DELETE FROM "+b.table+" WHERE key = ?", key)
	if err != nil {
		return er.Errorf("Error deleting from in [%s]: [%s]", &b.id, err)
	}
	return nil
}

//// Iterators

func (b *Bucket) ForEachBeginningWith(beginKey []byte, fn func(k, v []byte) er.R) er.R {
	bb := (*bbolt.Bucket)(b)
	tx := bb.Tx()
	if tx == nil || tx.DB() == nil {
		return walletdb.ErrTxClosed.Default()
	}
	c := bb.Cursor()
	var k, v []byte
	if len(beginKey) > 0 {
		k, v = c.Seek(beginKey)
	} else {
		k, v = c.First()
	}
	for ; k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// ForEach invokes the passed function with every key/value pair in the bucket.
// This includes nested buckets, in which case the value is nil, but it does not
// include the key/value pairs within those nested buckets.
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the walletdb.ReadBucket interface implementation.
func (b *Bucket) ForEach(fn func(k, v []byte) er.R) er.R {
	return b.ForEachBeginningWith(nil, fn)
}

func (b *Bucket) ReadCursor() walletdb.ReadCursor {
	return b.ReadWriteCursor()
}

// ReadWriteCursor returns a new cursor, allowing for iteration over the bucket's
// key/value pairs and nested buckets in forward or backward order.
//
// This function is part of the walletdb.ReadWriteBucket interface implementation.
func (b *Bucket) ReadWriteCursor() walletdb.ReadWriteCursor {
	return (*cursor)((*bbolt.Bucket)(b).Cursor())
}

// Tx returns the bucket's transaction.
//
// This function is part of the walletdb.ReadWriteBucket interface implementation.
func (b *Bucket) Tx() walletdb.ReadWriteTx {
	return b.txn
}
