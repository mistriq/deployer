// Package controlplane implements the private portal-to-deployer control plane.
package controlplane

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

type database struct {
	Projects map[string]*Project  `json:"projects"`
	Jobs     map[string]*Job      `json:"jobs"`
	Requests map[string]Receipt   `json:"requests"`
	Logs     map[string][]LogLine `json:"logs"`
}
type Receipt struct {
	Hash     string
	Response json.RawMessage
}
type Store struct {
	mu            sync.Mutex
	path          string
	aead          cipher.AEAD
	data          database
	lock          *os.File
	poisoned      bool
	syncDirectory func(string) error
}

// OpenStore requires a stable, externally supplied 32-byte encryption key. A process
// lock deliberately limits this version to one worker process per state directory.
func OpenStore(path string, key []byte) (*Store, error) {
	if len(key) != 32 {
		return nil, errors.New("state key must contain 32 bytes")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("state is already in use")
	}
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	s := &Store{path: path, aead: aead, lock: lock, data: database{Projects: map[string]*Project{}, Jobs: map[string]*Job{}, Requests: map[string]Receipt{}, Logs: map[string][]LogLine{}}}
	b, err := os.ReadFile(path)
	if err == nil {
		if len(b) < aead.NonceSize() {
			err = errors.New("invalid state")
		} else {
			var clear []byte
			clear, err = aead.Open(nil, b[:aead.NonceSize()], b[aead.NonceSize():], []byte("runtime-deployer-v1"))
			if err == nil {
				err = json.Unmarshal(clear, &s.data)
			}
		}
	}
	if err != nil && !os.IsNotExist(err) {
		s.Close()
		return nil, errors.New("cannot decrypt or read state")
	}
	return s, nil
}
func (s *Store) Close() error { return s.lock.Close() }
func (s *Store) persist() error {
	b, err := json.Marshal(s.data)
	if err != nil {
		return err
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}
	b = s.aead.Seal(nonce, nonce, b, []byte("runtime-deployer-v1"))
	f, err := os.CreateTemp(filepath.Dir(s.path), ".state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, s.path); err != nil {
		return err
	}
	// After rename, memory must retain the new state even if durability is uncertain.
	if s.syncDirectory != nil {
		err = s.syncDirectory(filepath.Dir(s.path))
		s.poisoned = err != nil
		return err
	}
	dir, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		s.poisoned = true
		return err
	}
	defer dir.Close()
	err = dir.Sync()
	s.poisoned = err != nil
	return err
}
func (s *Store) update(fn func(*database) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return errors.New("store durability uncertain; restart required")
	}
	before, _ := json.Marshal(s.data)
	if err := fn(&s.data); err != nil {
		s.data = database{}
		_ = json.Unmarshal(before, &s.data)
		return err
	}
	if err := s.persist(); err != nil {
		if !s.poisoned {
			s.data = database{}
			_ = json.Unmarshal(before, &s.data)
		}
		return err
	}
	return nil
}
func (s *Store) snapshot() database {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(s.data)
	var d database
	_ = json.Unmarshal(b, &d)
	return d
}
