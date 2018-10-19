package logevent

import (
	"testing"
)

func BenchmarkMarshalLogEvent(b *testing.B) {
	var le LogEvent
	le.Init(123456, tstStr)
	var store [200]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		le.Marshal(store[:])
	}
}

func BenchmarkUnmarshalLogEventFast(b *testing.B) {
	var le LogEvent
	le.InitWithTagLine(123456, tstStr, tstTags)
	var store [2000]byte
	le.Marshal(store[:])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		le.Unmarshal(store[:])
	}
}

func TestLogEventMarshalUnmarshal(t *testing.T) {
	var le LogEvent
	le.Init(123412341234123, "ha ha ha")
	leMarshalUnmarshal(t, &le)

	le.InitWithTagLine(12313, "abc", TagLine("abcd"))
	leMarshalUnmarshal(t, &le)

	le.tgid = 12349999999
	leMarshalUnmarshal(t, &le)
}

func leMarshalUnmarshal(t *testing.T, le *LogEvent) {
	bf := make([]byte, le.BufSize())
	n, err := le.Marshal(bf)
	if n != len(bf) || err != nil {
		t.Fatal("Expecting n=", n, " == ", len(bf), ", err=", err)
	}

	var le2 LogEvent
	n1, err := le2.Unmarshal(bf)
	if n != n1 || err != nil {
		t.Fatal("Expecting n1=", n, ", but it is ", n1, " or err is not nil: err=", err)
	}

	if le.GetMessage() != le2.GetMessage() || le.GetTimestamp() != le2.GetTimestamp() ||
		le.GetTagLine() != le2.GetTagLine() || le.GetTGroupId() != le2.GetTGroupId() {
		t.Fatal("Expecting le2=", le, ", but le2=", &le2)
	}
}

func TestMarshalUnmarshalTGIOnly(t *testing.T) {
	var le LogEvent
	le.Init(1234, WeakString("Hello"))
	bf := make([]byte, le.BufSize())
	n, err := le.Marshal(bf)
	if n != len(bf) || err != nil {
		t.Fatal("Expecting n=", n, " == ", len(bf), ", err=", err)
	}

	var le2 LogEvent
	le2.SetTGroupId(333)
	le2.MarshalTagGroupIdOnly(bf)
	n1, err := le2.Unmarshal(bf)
	if n != n1 || err != nil {
		t.Fatal("Expecting n1=", n, ", but it is ", n1, " or err is not nil: err=", err)
	}

	if le.GetMessage() != le2.GetMessage() || le.GetTimestamp() != le2.GetTimestamp() ||
		le.GetTagLine() != le2.GetTagLine() || le.GetTGroupId() == le2.GetTGroupId() || le.GetTGroupId() != 0 || le2.GetTGroupId() != 333 {
		t.Fatal("Expecting le2=", le, ", but le2=", &le2)
	}
}
