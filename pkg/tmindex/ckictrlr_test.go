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

package tmindex

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestCkiCtrlrInit(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestCkiCtrlrInit")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	cc := newCkiCtrlr()
	err = cc.init(dir, 2)
	if err != nil {
		t.Fatal("init must be ok, but err=", err)
	}

	idx, _ := cc.createIndex(1234)
	idx2, _ := cc.createIndex(1235)

	mp := cc.idxes.Load().(ckiMap)
	if mp[1234] != idx || mp[1235] != idx2 {
		t.Fatal("must be index here!, but mp=", mp)
	}

	err = cc.close()
	if err != nil {
		t.Fatal("close must be ok, but err=", err)
	}

	mp = cc.idxes.Load().(ckiMap)
	if len(mp) != 0 {
		t.Fatal("must be empty, but mp=", mp)
	}

	err = cc.init(dir, 1)
	if err != nil {
		t.Fatal("init must be ok, but err=", err)
	}

	idx = cc.getIndex(1234)
	idx2 = cc.getIndex(1235)

	mp = cc.idxes.Load().(ckiMap)
	if mp[1234] != idx || mp[1235] != idx2 {
		t.Fatal("must be index here!, but mp=", mp)
	}

	err = cc.close()
	if err != nil {
		t.Fatal("could not close err=", err)
	}
}

func TestCkiCtrlrOnWriteToFull(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestCkiCtrlrOnWriteToFull")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	cc := newCkiCtrlr()
	err = cc.init(dir, 2)
	if err != nil {
		t.Fatal("init must be ok, but err=", err)
	}

	root, err := cc.arrangeRoot(record{0, 0})
	if err != nil {
		t.Fatal("could not arrange root err=", err)
	}
	cnt := 0

	for err == nil {
		root, err = cc.onWrite(root, record{int64(cnt), uint32(cnt)})
		cnt++
	}

	fmt.Println(" ", cnt, " writes err=", err)

	if _, err = os.Stat(cc.getIndexFileName(root.IndexId)); !os.IsNotExist(err) {
		t.Fatal("should be no files for ", cc.getIndexFileName(root.IndexId), ", but err=", err)
	}
}

func TestCkiCtrlrOnWriteInTheLoop(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestCkiCtrlrOnWriteInTheLoop")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	cc := newCkiCtrlr()
	err = cc.init(dir, 1)
	if err != nil {
		t.Fatal("init must be ok, but err=", err)
	}

	for i := 0; i < 1000; i++ {
		root, err := cc.arrangeRoot(record{0, 0})
		if err != nil {
			t.Fatal("could not arrange root err=", err)
		}

		for cnt := 0; cnt < 1000; cnt++ {
			root, err = cc.onWrite(root, record{int64(cnt), uint32(cnt)})
			if err != nil {
				t.Fatal("something wrong with onWrite err=", err)
			}
		}

		idx := cc.getIndex(root.IndexId)
		if idx == nil {
			t.Fatal("expected to have the index, but it is nil")
		}

		if idx.bks.Available() == idx.bks.Count() {
			t.Fatal("wrong index state ", idx)
		}

		cc.removeItem(root)
		if idx.bks.Available() != idx.bks.Count() {
			t.Fatal("wrong index state ", idx)
		}
	}
	cc.close()
}

func TestCkiCtrlrNewIndex(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestCkiCtrlrNewIndex")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	cc := newCkiCtrlr()
	err = cc.init(dir, 1)
	if err != nil {
		t.Fatal("init must be ok, but err=", err)
	}

	for {
		root, err := cc.arrangeRoot(record{0, 0})
		if err != nil {
			t.Fatal("could not arrange root err=", err)
		}

		for cnt := 0; cnt < 1000; cnt++ {
			root, err = cc.onWrite(root, record{int64(cnt), uint32(cnt)})
			if err != nil {
				t.Fatal("something wrong with onWrite err=", err)
			}
		}

		mp := cc.idxes.Load().(ckiMap)
		if len(mp) > 2 {
			break
		}

	}
	cc.close()
}
