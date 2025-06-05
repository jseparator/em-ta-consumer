package main

import (
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"
)

const (
	RED    = "\033[0;31m"
	GREEN  = "\033[1;32m"
	YELLOW = "\033[1;33m"
	BLUE   = "\033[1;34m"
	NC     = "\033[0m"
)

func findProc(pidFile string) *os.Process {
	pid, err := os.ReadFile(pidFile)
	if err != nil || len(pid) == 0 {
		return nil
	}
	id, err := strconv.Atoi(string(pid))
	if err != nil {
		return nil
	}
	if id == os.Getpid() {
		return nil
	}
	proc, _ := os.FindProcess(id)
	return proc
}

func isAlive(proc *os.Process) bool {
	if proc == nil {
		return false
	}
	return proc.Signal(syscall.SIGUSR1) == nil
}

func kill(proc *os.Process) {
	if proc == nil {
		return
	}
	for {
		err := proc.Signal(syscall.SIGINT)
		if err != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func daemonize(pidFile string) {
	if os.Getenv("__FORK_PROC__") == "1" {
		return // 子进程直接返回
	}
	childPid, err := syscall.ForkExec(os.Args[0], os.Args, &syscall.ProcAttr{
		Files: []uintptr{os.Stdin.Fd(), os.Stdout.Fd(), os.Stderr.Fd()},
		Env:   append(os.Environ(), "__FORK_PROC__=1"),
	})
	if err != nil {
		fmt.Printf("%sfailed to daemonize:%s %v\n", RED, NC, err)
		os.Exit(1)
	}

	fmt.Printf("%sdaemon started with PID%s %d%s\n", BLUE, GREEN, childPid, NC)
	err = os.WriteFile(pidFile, []byte(strconv.Itoa(childPid)), 0644)
	if err != nil {
		fmt.Printf("%sfailed to write PID:%s %v\n", RED, NC, err)
		os.Exit(1)
	}

	os.Exit(0) // 父进程退出
}
