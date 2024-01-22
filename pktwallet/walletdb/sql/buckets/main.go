package buckets

import (
	"database/sql"

	"github.com/pkt-cash/pktd/btcutil/er"
)

func Create(tx *sql.Tx) er.R {
	_, err := tx.Exec("CREATE TABLE bucketindex(" +
		"path     blob PRIMARY KEY," +
		"table    text UNIQUE NOT NULL," +
		"sequence integer NOT NULL" +
		")")
	return er.E(err)
}

func Destroy(tx *sql.Tx) er.R {
	_, err := tx.Exec("DROP TABLE bucketindex")
	return er.E(err)
}

func getTableName(sqltx *sql.Tx, path *bucketId) (string, er.R) {
	row := sqltx.QueryRow("SELECT table FROM bucketindex WHERE path = ?", path.bytes)
	var table string
	if err := row.Scan(&table); err == nil {
		// exists
		return table, nil
	} else if sql.ErrNoRows != err {
		return "", er.E(err)
	} else {
		return "", nil
	}
}

func mkBucket(sqltx *sql.Tx, path bucketId, create bool) (*Bucket, er.R) {
	table, err := getTableName(sqltx, &path)
	if err != nil {
		return nil, err
	} else if table != "" {
		return &Bucket{id: path, table: table, sqltx: sqltx}, nil
	} else if !create {
		return nil, nil
	}
	// does not yet exist, create it
	unique := 0
	for {
		table, err = encodeTableName(&path, unique)
		if err != nil {
			return nil, err
		}
		_, err := sqltx.Exec(
			"INSERT OR IGNORE INTO bucketindex (path, table, sequence) VALUES (?, ?, ?)", path, table, 0)
		if err != nil {
			return nil, er.E(err)
		}
		if table1, err := getTableName(sqltx, &path); err != nil {
			return nil, err
		} else if table1 == table {
			break
		}
		// table name collision
		unique++
	}

	// create the bucket table
	if _, err := sqltx.Exec("CREATE TABLE " + table + " ( " +
		"key blob PRIMARY KEY NOT NULL," +
		"val blob NOT NULL" +
		")"); err != nil {
		return nil, er.E(err)
	}
	return &Bucket{id: path, table: table, sqltx: sqltx}, nil
}

func destroyBucket(sqltx *sql.Tx, path *bucketId) er.R {
	// Drop table
	// Delete table entry from index table
	table, err := getTableName(sqltx, path)
	if err != nil {
		return err
	} else if table == "" {
		return er.Errorf("Unable to destroy bucket [%s] it does not exist", table)
	}
	if _, err := sqltx.Exec("DROP TABLE " + table); err != nil {
		return er.E(err)
	}
	if _, err := sqltx.Exec("DELETE FROM bucketindex WHERE path = ?", path); err != nil {
		return er.E(err)
	}
	return nil
}

func RootBucket(sqltx *sql.Tx, key []byte, create bool) (*Bucket, er.R) {
	return mkBucket(sqltx, encodeIndexKey(key), create)
}
