package app

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type deployerInstanceLock struct {
	file *os.File
}

func acquireDeployerInstanceLock(dbPath string) (*deployerInstanceLock, error) {
	if dbPath == ":memory:" {
		return nil, fmt.Errorf("in-memory database cannot provide exclusive Deployer instance ownership")
	}
	absoluteDBPath, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, fmt.Errorf("resolve database path for instance lock: %w", err)
	}
	file, err := os.OpenFile(absoluteDBPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open database for Deployer instance lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("another Deployer process already owns database %s: %w", absoluteDBPath, err)
	}
	return &deployerInstanceLock{file: file}, nil
}

func acquireHostingAgentInstanceLock(workRoot string) (*deployerInstanceLock, error) {
	absoluteRoot, err := filepath.Abs(workRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve hosting-agent work root for instance lock: %w", err)
	}
	file, err := os.Open(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("open hosting-agent work root for instance lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("another hosting-agent process already owns work root %s: %w", absoluteRoot, err)
	}
	return &deployerInstanceLock{file: file}, nil
}

func (lock *deployerInstanceLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	err := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	closeErr := lock.file.Close()
	lock.file = nil
	if err != nil {
		return err
	}
	return closeErr
}
