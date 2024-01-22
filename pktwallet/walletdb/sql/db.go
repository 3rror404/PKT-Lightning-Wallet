package sql

import (
	"context"
	"database/sql"
	"io"

	"github.com/pkt-cash/pktd/btcutil/er"
	"github.com/pkt-cash/pktd/pktwallet/walletdb"
)

type db_t struct {
	sqldb sql.DB
}

var _ walletdb.DB = (*db_t)(nil)

func (db *db_t) beginTx(readOnly bool) (*transaction_t, er.R) {
	sqltx, err := db.sqldb.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: readOnly})
	if err != nil {
		return nil, er.E(err)
	}
	return &transaction_t{
		sqltx: sqltx,
	}, nil
}

func (db *db_t) BeginReadTx() (walletdb.ReadTx, er.R) {
	return db.beginTx(true)
}

func (db *db_t) BeginReadWriteTx() (walletdb.ReadWriteTx, er.R) {
	return db.beginTx(false)
}

func (db *db_t) Copy(w io.Writer) er.R {
	panic("unimplemented")
}

func (db *db_t) Close() er.R {
	return er.E(db.sqldb.Close())
}
