package bucketpath

type BucketPath struct {
	path  [][]byte
	table string
}

func (bp *BucketPath) Child(name []byte) BucketPath {
	path := append(make([][]byte, 0, len(bp.path)+1), bp.path...)
	path = append(path, name)
	return BucketPath{path: path, table: tableName(path)}
}

func (bp *BucketPath) Table() string {
	return bp.table
}

func RootKey(name []byte) BucketPath {
	return BucketPath{path: [][]byte{name}}
}
