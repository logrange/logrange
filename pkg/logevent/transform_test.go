package logevent

import (
	"reflect"
	"testing"
)

var tstStr = WeakString("This is some string for test marshalling speed Yahhoooo 11111111111111111111111111111111111111111111111111")
var tstTags = TagLine("pod=1234134kjhakfdjhlakjdsfhkjahdlf,key=abc,")

func BenchmarkMarshalString(b *testing.B) {
	var buf [200]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalString(string(tstStr), buf[:])
	}
}

func BenchmarkUnmarshalString(b *testing.B) {
	var buf [200]byte
	MarshalString(string(tstStr), buf[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnmarshalString(buf[:])
	}
}

func BenchmarkWeakStringCast(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := string(tstStr)
		tstStr = WeakString(s)
	}
}

func BenchmarkWeakStringCastByCopy(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := tstStr.String()
		tstStr = WeakString(s)
	}
}

func TestStrSliceToSSlice(t *testing.T) {
	ss := []string{"a", "b", "c"}
	s := StrSliceToSSlice(ss)
	if len(s) != 3 || s[1] != "b" {
		t.Fatal("cannot cast s=", s)
	}

	if reflect.TypeOf(s[0]).String() != "logevent.WeakString" {
		t.Fatal("oops, some wrong underlying type ", reflect.TypeOf(s[0]))
	}
}

func TestMarshalString(t *testing.T) {
	str := "hello str"
	buf := make([]byte, len(str)+4)
	n, err := MarshalString(str, buf)
	if err != nil {
		t.Fatal("Should be enough space, but err=", err)
	}
	if n != len(str)+4 {
		t.Fatal("expected string marshal size is ", len(str)+4, ", but actual is ", n)
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	str := "hello str"
	buf := make([]byte, len(str)+4)
	MarshalString(str, buf)
	_, str2, _ := UnmarshalString(buf)
	if string(str2) != str {
		t.Fatal("Wrong unmarshaling str=", str, ", str2=", str2)
	}

	buf[4] = buf[5]
	if string(str2) == str {
		t.Fatal("They must be different now: str=", str, ", str2=", str2)
	}
}

func TestMarshalUnmarshalSSliceEmpty(t *testing.T) {
	ss := []WeakString{}
	buf := make([]byte, 100)
	n, err := MarshalSSlice(SSlice(ss), buf)
	if n != 2 || err != nil {
		t.Fatal("Must be able to write 2 bytes for the SSlice length n=", n, ", err=", err)
	}

	ss, n, err = UnmarshalSSlice(ss, buf)
	if n != 2 || err != nil || len(ss) != 0 {
		t.Fatal("Must be able to read 2 bytes for the SSlice length n=", n, ", err=", err)
	}
}

func TestMarshalUnmarshalSSlice(t *testing.T) {
	ss := []WeakString{"aaa", "bbb"}

	if SSlice(ss).Size() != 16 {
		t.Fatal("Expecting marshaled size 16, but really ", SSlice(ss).Size())
	}

	buf := make([]byte, 100)
	n, err := MarshalSSlice(SSlice(ss), buf)
	if n != 16 || err != nil {
		t.Fatal("Must be able to write 16 bytes for the SSlice length n=", n, ", err=", err)
	}

	s := []WeakString{"", ""}
	ss, n, err = UnmarshalSSlice(SSlice(s[:1]), buf)
	if n != 16 || err != nil || len(ss) != 2 || ss[0] != "aaa" || ss[1] != "bbb" {
		t.Fatal("Must be able to read 2 bytes for the SSlice length n=", n, ", err=", err, ", ss=", ss)
	}

	s = []WeakString{""}
	ss, n, err = UnmarshalSSlice(SSlice(s), buf)
	if err == nil {
		t.Fatal("Must report error - slice not big enough!")
	}
}

func TestMarshalStringBuf(t *testing.T) {
	str := "Hello World"
	b := []byte{}
	n, err := MarshalStringBuf(str, b)
	if err == nil {
		t.Fatal("should be error, but it is not, n=", n)
	}

	b = make([]byte, 100)
	n, err = MarshalStringBuf(str, b)
	if err != nil || n != len(str) {
		t.Fatal("should not be an error, but n=", n, ", err=", err)
	}

	b[0] = 'h'
	str0 := UnmarshalStringBuf(b[:n]).String()
	if str0 == str || str0 != "hello World" {
		t.Fatal("Unexpected values str=", str, ", str0=", str0)
	}
}
