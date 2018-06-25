package storage

import (
	"crypto/rand"
	"fmt"
	"io"
	"time"

	"github.com/coreos/bbolt"
	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/crypto/nacl/secretbox"
	"golang.org/x/crypto/scrypt"
)

var internalBucketName = []byte("_internal")
var saltKey = []byte("salt")

type EncryptedBoltStorage struct {
	db        *bolt.DB
	secretKey *[32]byte
}

var _ EncryptedStorage = (*EncryptedBoltStorage)(nil)

func NewEncryptedBoltStorage(path string) *EncryptedBoltStorage {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		log.Crit("error opening db", "error", err)
	}
	ebs := &EncryptedBoltStorage{
		db: db,
	}
	err = ebs.CreateBucketIfNotExists(internalBucketName)
	if err != nil {
		panic(err)
	}

	return ebs
}

func (ebs *EncryptedBoltStorage) Close() {
	ebs.db.Close()
}

func (ebs *EncryptedBoltStorage) Unlock(passphrase string) {
	var key [32]byte
	copy(key[:], ebs.PassphraseToKey(passphrase))
	ebs.secretKey = &key
}

func (ebs *EncryptedBoltStorage) PassphraseToKey(passphrase string) []byte {
	dk, err := scrypt.Key([]byte(passphrase), ebs.getSalt(), 32768, 8, 1, 32)
	if err != nil {
		panic(err)
	}
	return dk
}

func (ebs *EncryptedBoltStorage) getSalt() []byte {
	salt := ebs.getUnencrypted(internalBucketName, saltKey)
	if salt == nil {
		salt = make([]byte, 8)
		if _, err := io.ReadFull(rand.Reader, salt); err != nil {
			panic(err)
		}
		if err := ebs.setUnencrypted(internalBucketName, saltKey, salt); err != nil {
			panic(err)
		}
	}

	return salt
}

func (ebs *EncryptedBoltStorage) CreateBucketIfNotExists(bucketName []byte) error {
	return ebs.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
}

func (ebs *EncryptedBoltStorage) Set(bucketName []byte, key []byte, value []byte) error {
	log.Debug("setting value for ", string(bucketName), string(key))

	var nonce [24]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return fmt.Errorf("error getting nonce: %v", err)
	}

	encryptedValue := secretbox.Seal(nonce[:], value, &nonce, ebs.secretKey)
	if encryptedValue == nil {
		return fmt.Errorf("error setting")
	}

	return ebs.setUnencrypted(bucketName, key, encryptedValue)
}

func (ebs *EncryptedBoltStorage) Delete(bucketName []byte, key []byte) error {
	return ebs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		err := b.Delete([]byte(key))
		return err
	})
}

func (ebs *EncryptedBoltStorage) Get(bucketName []byte, key []byte) ([]byte, error) {
	log.Debug("getting value for ", string(bucketName), string(key))

	encryptedBytes := ebs.getUnencrypted(bucketName, key)
	if len(encryptedBytes) == 0 {
		return nil, nil
	}
	var decryptNonce [24]byte
	copy(decryptNonce[:], encryptedBytes[:24])
	decrypted, ok := secretbox.Open(nil, encryptedBytes[24:], &decryptNonce, ebs.secretKey)
	if !ok {
		return nil, fmt.Errorf("error decrypting")
	}
	return decrypted, nil
}

func (ebs *EncryptedBoltStorage) GetKeys(bucketName []byte) ([][]byte, error) {
	var keys [][]byte
	ebs.db.View(func(tx *bolt.Tx) error {
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

func (ebs *EncryptedBoltStorage) ForEach(bucketName []byte, iterator func(k, v []byte) error) error {
	err := ebs.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(bucketName)

		err := b.ForEach(iterator)

		return err
	})
	return err
}

func (ebs *EncryptedBoltStorage) setUnencrypted(bucketName []byte, key []byte, value []byte) error {
	return ebs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		err := b.Put([]byte(key), value)
		return err
	})
}

func (ebs *EncryptedBoltStorage) getUnencrypted(bucketName []byte, key []byte) []byte {
	var valueBytes []byte
	ebs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		res := b.Get([]byte(key))
		valueBytes = make([]byte, len(res))

		copy(valueBytes, res)
		return nil
	})
	return valueBytes
}
