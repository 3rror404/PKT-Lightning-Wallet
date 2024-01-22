package bucketpath

import "encoding/hex"

func tableName(path [][]byte) string {
	out := "bkt"
	for _, x := range path {
		out += "_"
		for _, chr := range x {
			out += escapeChar(chr)
		}
	}
	return out
}

func escapeChar(c byte) string {
	if c >= '0' && c <= '9' {
		return string(c)
	} else if c >= 'A' && c <= 'Z' {
		return string(c)
	} else if c >= 'a' && c <= 'z' {
		return string(c)
	} else {
		return "%" + hex.EncodeToString([]byte{c})
	}
}
