package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/quorumcontrol/qc3/storage"
	"github.com/stretchr/testify/assert"
)

func TestEncryptedBoltStorage_PassphraseToKey(t *testing.T) {
	os.RemoveAll("testtmp")
	os.MkdirAll("testtmp", 0700)
	defer os.RemoveAll("testtmp")
	ebs := storage.NewEncryptedBoltStorage(filepath.Join("testtmp", "testdb"))
	assert.Len(t, ebs.PassphraseToKey("somepassword"), 32)
}

func TestEncryptedBoltStorage_SetGet(t *testing.T) {
	os.RemoveAll("testtmp")
	os.MkdirAll("testtmp", 0700)
	defer os.RemoveAll("testtmp")
	ebs := storage.NewEncryptedBoltStorage(filepath.Join("testtmp", "testdb"))
	ebs.Unlock("test")

	ebs.CreateBucketIfNotExists([]byte("test"))

	err := ebs.Set([]byte("test"), []byte("key"), []byte("value"))
	assert.Nil(t, err)
	val, err := ebs.Get([]byte("test"), []byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, val, []byte("value"))
}

func TestEncryptedBoltStorage_GetKeys(t *testing.T) {
	os.RemoveAll("testtmp")
	os.MkdirAll("testtmp", 0700)
	defer os.RemoveAll("testtmp")
	ebs := storage.NewEncryptedBoltStorage(filepath.Join("testtmp", "testdb"))
	ebs.Unlock("test")

	ebs.CreateBucketIfNotExists([]byte("test"))

	err := ebs.Set([]byte("test"), []byte("key"), []byte("value"))
	assert.Nil(t, err)
	keys, err := ebs.GetKeys([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, keys, [][]byte{[]byte("key")})
}
