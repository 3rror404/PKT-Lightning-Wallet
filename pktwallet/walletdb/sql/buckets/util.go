package buckets

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/pkt-cash/pktd/btcutil/er"
	"github.com/pkt-cash/pktd/wire"
)

type bucketId struct {
	bytes []byte
}

func (bid *bucketId) String() string {
	if table, err := encodeTableName(bid, 0); err != nil {
		return fmt.Sprintf("Error encoding bucket name: [%s]: [%s]", hex.EncodeToString(bid.bytes), err)
	} else {
		return table
	}
}

func (bid *bucketId) withAppended(buf []byte) bucketId {
	return bucketId{append(append([]byte{}, bid.bytes...), mkRootBucketId(buf).bytes...)}
}

func mkRootBucketId(path []byte) bucketId {
	var tx bytes.Buffer
	wire.WriteVarBytes(&tx, 0, path)
	return bucketId{tx.Bytes()}
}

// unique should be used to make the table name unique if it collides with an existing table.
// otherwise leave it zero.
func encodeTableName(path *bucketId, unique int) (string, er.R) {
	buf := bytes.NewBuffer(path.bytes)
	out := "bkt"
	for {
		x, err := wire.ReadVarBytes(buf, 0, 1000, "<path>")
		if err != nil {
			return "", err
		}
		out += "-"
		for _, chr := range x {
			out += escapeChar(chr)
		}
		if buf.Len() == 0 {
			break
		}
	}
	u := ""
	if unique > 0 {
		u = "-u" + fmt.Sprintf("%x", unique)
	}
	if len(out)+len(u) > 60 {
		out = out[0 : 60-len(u)]
	}
	return out + u, nil
}

func escapeChar(c byte) string {
	if c >= '0' && c <= '9' {
		return string(c)
	} else if c >= 'A' && c <= 'Z' {
		return string(c)
	} else if c >= 'a' && c <= 'z' {
		return string(c)
	} else {
		// It's ok for this to be lossy because it is deduplicated
		return "_"
	}
}
