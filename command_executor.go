package main

import (
	"context"
	"io"
	"os"
	"os/exec"
)

type runningCommand interface {
	Wait() error
}

type commandExecutor interface {
	Start(ctx context.Context, dir string, name string, args ...string) (runningCommand, io.ReadCloser, error)
	Run(ctx context.Context, dir string, name string, args ...string) error
	CombinedOutput(ctx context.Context, dir string, name string, args ...string) ([]byte, error)
	Output(ctx context.Context, dir string, name string, args ...string) ([]byte, error)
}

type osCommandExecutor struct{}

var systemCommands commandExecutor = osCommandExecutor{}

func (osCommandExecutor) Start(ctx context.Context, dir string, name string, args ...string) (runningCommand, io.ReadCloser, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}

	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	cmd.Stdout = pw
	cmd.Stderr = pw

	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return nil, nil, err
	}
	pw.Close()
	return cmd, pr, nil
}

func (osCommandExecutor) Run(ctx context.Context, dir string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	return cmd.Run()
}

func (osCommandExecutor) CombinedOutput(ctx context.Context, dir string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	return cmd.CombinedOutput()
}

func (osCommandExecutor) Output(ctx context.Context, dir string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	return cmd.Output()
}
