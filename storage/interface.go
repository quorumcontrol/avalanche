package storage

type Storage interface {
	Close()
	CreateBucketIfNotExists(bucketName []byte) error
	Set(bucketName []byte, key []byte, value []byte) error
	Get(bucketName []byte, key []byte) ([]byte, error)
	Delete(bucketName []byte, key []byte) error
	GetKeys(bucketName []byte) ([][]byte, error)
	ForEach(bucketName []byte, iterator func(k, v []byte) error) error
}

type EncryptedStorage interface {
	Storage
	Unlock(passphrase string)
}
