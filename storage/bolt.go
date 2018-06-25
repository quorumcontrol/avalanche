package storage

import (
	"time"

	"github.com/coreos/bbolt"
	"github.com/ethereum/go-ethereum/log"
)

type BoltStorage struct {
	db *bolt.DB
}

var _ Storage = (*BoltStorage)(nil)

func NewBoltStorage(path string) *BoltStorage {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		log.Crit("error opening db", "error", err)
	}

	bs := &BoltStorage{db: db}

	return bs
}

func (bs *BoltStorage) Close() {
	bs.db.Close()
}

func (bs *BoltStorage) CreateBucketIfNotExists(bucketName []byte) error {
	return bs.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
}

func (bs *BoltStorage) Set(bucketName []byte, key []byte, value []byte) error {
	return bs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		err := b.Put([]byte(key), value)
		return err
	})
}

func (bs *BoltStorage) Delete(bucketName []byte, key []byte) error {
	return bs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		err := b.Delete([]byte(key))
		return err
	})
}

func (bs *BoltStorage) Get(bucketName []byte, key []byte) ([]byte, error) {
	var valueBytes []byte
	bs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		res := b.Get([]byte(key))
		valueBytes = make([]byte, len(res))

		copy(valueBytes, res)
		return nil
	})
	return valueBytes, nil
}

func (bs *BoltStorage) GetKeys(bucketName []byte) ([][]byte, error) {
	var keys [][]byte
	bs.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(bucketName)

		b.ForEach(func(k, _ []byte) error {
			keys = append(keys, k)
			return nil
		})

		return nil
	})
	return keys, nil
}

func (bs *BoltStorage) ForEach(bucketName []byte, iterator func(k, v []byte) error) error {
	err := bs.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(bucketName)

		err := b.ForEach(iterator)

		return err
	})
	return err
}
