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

package container

import (
	"testing"
)

type __bchmrkVal int

func BenchmarkCLElement_Append(b *testing.B) {
	var l1, l *CLElement
	for i := 0; i < 1000; i++ {
		l1 = l1.Append(NewCLElement())
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if l1 == nil {
			l1 = l
			l = nil
		}
		l2 := l1.next
		l1 = l1.TearOff(l2)
		//l2.Val = __bchmrkVal{i}
		l = l.Append(l2)
	}
}

func TestNewCLElement(t *testing.T) {
	l := NewCLElement()
	if l.prev != l.next || l.prev != l {
		t.Fatal("Must be refer to itself")
	}
}

func TestCLElementLen(t *testing.T) {
	var l *CLElement
	if l.Len() != 0 {
		t.Fatal("for nil element Len() must be 0, but it is ", l.Len())
	}

	l = NewCLElement()
	if l.Len() != 1 {
		t.Fatal("for 1 element Len() must be 1, but it is ", l.Len())
	}
}

func TestCLElementAppend(t *testing.T) {
	var l, l1 *CLElement
	if l.Append(nil) != nil {
		t.Fatal("must be nil, but ", l.Append(nil))
	}

	l1 = NewCLElement()
	if l.Append(l1) != l1 || l1.Append(nil) != l1 || l.Len() != 0 || l1.Len() != 1 {
		t.Fatal("l still be nil, but l1 is not ", l, l1)
	}

	l = NewCLElement()
	l.Val = 1
	l = l.Append(NewCLElement())
	l.next.Val = 2
	l1.Val = 0
	if l.Len() != 2 {
		t.Fatal("l1 must be Len()==2, but l1.Len()=", l1.Len())
	}

	l1.Append(l)
	for i := 0; i < 3; i++ {
		if l1.Val != i {
			t.Fatal("expecting l1.Val=", i, ", but it is ", l1.Val)
		}
		l1 = l1.next
	}
	if l1.Len() != 3 || l.Len() != 3 {
		t.Fatal("Now all lists must be Len()==3")
	}
}

func TestCLElementTearOff(t *testing.T) {
	var l *CLElement
	if l.TearOff(nil) != nil {
		t.Fatal("Paradox")
	}
	l = NewCLElement()
	if l.TearOff(nil) != l {
		t.Fatal("something wrong")
	}

	if l.TearOff(l) != nil {
		t.Fatal("Must be nil")
	}

	l.Append(NewCLElement())
	if l.Len() != 2 {
		t.Fatal("Must be 2 elements")
	}

	n := l.next
	l2 := l.TearOff(l)
	if l2 != n || l2.Len() != 1 || l.Len() != 1 {
		t.Fatal("must be n==l2")
	}

	l2.Append(l)
	l2 = l2.TearOff(l)
	if l2 == l || l.Len() != 1 || l2.Len() != 1 {
		t.Fatal("something wrong")
	}
}
