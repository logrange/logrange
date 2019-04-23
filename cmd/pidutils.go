// Copyright 2018-2019 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"github.com/gofrs/flock"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type PidFile struct {
	fn string
	fl *flock.Flock
}

// NewPidFile creates new PidFile struct by the file name
func NewPidFile(fn string) *PidFile {
	return &PidFile{fn: fn}
}

// Interrupt tries to read the pid file and interrupt the process by its Pid, if possible
func (pf *PidFile) Interrupt() error {
	pid, err := pf.ReadPid()
	if err != nil {
		return err
	}

	if pid == -1 {
		return fmt.Errorf("not running")
	}

	p, err := os.FindProcess(pid)
	if err != nil {
		fmt.Printf("Error: there is a process pid=%d, but could not access to the process\n", pid)
		return err
	}

	err = p.Signal(os.Interrupt)
	if err != nil {
		fmt.Printf("Error: could not send signal to pid=%d\n", pid)
		return err
	}
	fmt.Println("Sending interrupt notification to process pid=", pid)
	return nil
}

// ReadPid tries to read the pid file and returns pid value, if possible
func (pf *PidFile) ReadPid() (int, error) {
	res, err := ioutil.ReadFile(pf.fn)
	if err != nil {
		return -1, nil
	}

	content := string(res)
	if len(content) > 10 {
		return -1, fmt.Errorf("wrong content of %s", pf.fn)
	}

	pid, err := strconv.ParseInt(content, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("could not parse content=\"%s\" of the file %s", content, pf.fn)
	}
	return int(pid), nil
}

// Lock tries to acquire the pid file and write the current process Id there. Returns true, if the
// operaion was successful.
func (pf *PidFile) Lock() bool {
	if pf.fl != nil {
		panic("Lock() must not be called twice")
	}

	plock := flock.New(pf.fn)

	if l, err := plock.TryLock(); !l || err != nil {
		fmt.Println("Error: Could not get lock for ", pf.fn)
		return false
	}

	if err := pf.writePid(); err != nil {
		fmt.Println("Error: Could not write current pid to ", pf.fn, ", err=", err)
		plock.Unlock()
		return false
	}
	fmt.Println("Succesfully locked pid file ", pf.fn)
	pf.fl = plock
	return true
}

// Unlock releases resources acquired by Lock.
func (pf *PidFile) Unlock() {
	if pf.fl == nil {
		panic("Must be locked!")
	}
	os.Remove(pf.fn)
	pf.fl.Unlock()
	pf.fl = nil
}

func (pf *PidFile) writePid() error {
	return ioutil.WriteFile(pf.fn, []byte(fmt.Sprintf("%d", os.Getpid())), 0640)
}

// RemoveArgsWithName removes all args from the list where there is the word name,
// it return new slice
func RemoveArgsWithName(args []string, name string) []string {
	name = strings.ToLower(name)
	if len(name) == 0 {
		return args
	}

	res := make([]string, 0, len(args))
	for _, a := range args {
		if strings.Contains(strings.ToLower(a), name) {
			continue
		}
		res = append(res, a)
	}

	return res
}

// RunCommand runs the command cmd with params, returns an error if any
func RunCommand(c string, params ...string) error {
	fmt.Printf("Starting command %s with params %v ... \n", c, params)
	cmd := exec.Command(c, params...)
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("Could not run command %s with params=%v error=%s", c, params, err)
	}

	sigChan := make(chan os.Signal, 1)
	defer signal.Stop(sigChan)

	signal.Notify(sigChan, syscall.SIGCHLD)
	select {
	case <-sigChan:
		err = fmt.Errorf("The process could not be started for a reason.")
	case <-time.After(time.Second):
		fmt.Printf("Started. pid=%d\n", cmd.Process.Pid)
	}

	return err
}
