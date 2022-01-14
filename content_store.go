package main

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

var (
	errHashMismatch = errors.New("Content hash does not match OID")
	errSizeMismatch = errors.New("Content size does not match")
)

// ContentStore provides a simple file system based storage.
type ContentStore struct {
	basePath string
}

// NewContentStore creates a ContentStore at the base directory.
func NewContentStore(base string) (*ContentStore, error) {
	if err := os.MkdirAll(base, 0750); err != nil {
		return nil, err
	}

	return &ContentStore{base}, nil
}

type bothCloser struct {
	f *os.File
	g *gzip.Reader
}

func (b *bothCloser) Read(p []byte) (int, error) {
	return b.g.Read(p)
}

func (b *bothCloser) Close() error {
	err := b.g.Close()
	if err := b.f.Close(); err != nil {
		return err
	}
	return err
}

// Get takes a Meta object and retreives the content from the store, returning
// it as an io.ReaderCloser. If fromByte > 0, the reader starts from that byte
func (s *ContentStore) Get(meta *MetaObject, fromByte int64) (io.ReadCloser, error) {
	path := filepath.Join(s.basePath, transformKey(meta.Oid)) + ".gz"

	fmt.Printf("Get %q\n", path)

	f, err := os.Open(path)
	if err != nil {
		fmt.Printf("failed to open %q %v\n", path, err)
		return nil, err
	}
	g, err := gzip.NewReader(f)
	if err != nil {
		fmt.Printf("file not gzip %s %v\n", path, err)
		return nil, err
	}
	if fromByte > 0 {
		_, err = io.CopyN(ioutil.Discard, g, fromByte)
		if err != nil {
			fmt.Printf("not enough bytes %s %v\n", path, err)
		}
	}
	return &bothCloser{f, g}, err
}

// Put takes a Meta object and an io.Reader and writes the content to the store.
func (s *ContentStore) Put(meta *MetaObject, r io.Reader) error {
	path := filepath.Join(s.basePath, transformKey(meta.Oid)) + ".gz"
	tmpPath := path + ".tmp"

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0640)
	if err != nil {
		return err
	}
	defer os.Remove(tmpPath)

	g, _ := gzip.NewWriterLevel(file, gzip.BestCompression)

	hash := sha256.New()
	hw := io.MultiWriter(hash, g)

	written, err := io.Copy(hw, r)
	if err != nil {
		fmt.Printf("failed to write %s %v\n", path, err)
		file.Close()
		return err
	}
	if err := g.Close(); err != nil {
		fmt.Printf("failed to close %s %v\n", path, err)
		file.Close()
		return err
	}
	file.Close()

	if written != meta.Size {
		return errSizeMismatch
	}

	shaStr := hex.EncodeToString(hash.Sum(nil))
	if shaStr != meta.Oid {
		return errHashMismatch
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

// Exists returns true if the object exists in the content store.
func (s *ContentStore) Exists(meta *MetaObject) bool {
	path := filepath.Join(s.basePath, transformKey(meta.Oid)) + ".gz"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func transformKey(key string) string {
	if len(key) < 5 {
		return key
	}

	return filepath.Join(key[0:2], key[2:4], key[4:len(key)])
}
