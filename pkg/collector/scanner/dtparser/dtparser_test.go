package dtparser

import (
	"testing"
)

func TestParser(t *testing.T) {
	p := NewDefaultParser()
	_, pf := p.Parse([]byte("jopasdf govnoa Tue Jan 30 00:42:28.694 <kernel> Setting BTCoex Config: enable_2G:1, profile_2g:0, enable_5G:1, profile_5G:0"), nil)
	if pf == nil {
		t.Fatal("Must found :(, but could not")
	}
}
