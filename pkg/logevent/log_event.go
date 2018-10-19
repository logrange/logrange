package logevent

import (
	"fmt"
)

type (
	LogEvent struct {
		tgid int64
		ts   int64
		msg  WeakString
		tgl  WeakString
	}
)

func (le *LogEvent) Init(ts int64, msg WeakString) {
	le.ts = ts
	le.msg = msg
}

func (le *LogEvent) InitWithTagLine(ts int64, msg WeakString, tgl TagLine) {
	le.ts = ts
	le.msg = msg
	le.tgl = WeakString(tgl)
}

// GetTimestamp returns timestamp in nanoseconds. It could be negative if
// it less than 01/01/1970
func (le *LogEvent) GetTimestamp() int64 {
	return le.ts
}

func (le *LogEvent) GetMessage() WeakString {
	return le.msg
}

func (le *LogEvent) GetTagLine() TagLine {
	return TagLine(le.tgl.String())
}

func (le *LogEvent) GetTGroupId() int64 {
	return le.tgid
}

func (le *LogEvent) SetTGroupId(id int64) {
	le.tgid = id
}

// BufSize returns size of marshalled data
func (le *LogEvent) BufSize() int {
	if len(le.tgl) == 0 {
		// tgid(8bts)+ ts(8bts) + msgLen(4 bts) + msg
		return 20 + len(le.msg)
	}
	// tgid(8bts)+ ts(8bts) + msgLen(4 bts) + msg + tglLen(4 bts) + tgl
	return 24 + len(le.msg) + len(le.tgl)
}

// MarshalTagGroupIdOnly marshals tagId from the le to provided buffer supposing
// that the buf is marshalled event
func (le *LogEvent) MarshalTagGroupIdOnly(buf []byte) (int, error) {
	return MarshalInt64(int64(le.tgid), buf)
}

func (le *LogEvent) Marshal(buf []byte) (int, error) {
	n, err := MarshalInt64(int64(le.tgid), buf)
	if err != nil {
		return 0, err
	}

	n1, err := MarshalInt64(le.ts, buf[n:])
	if err != nil {
		return 0, err
	}
	n += n1

	n1, err = MarshalString(string(le.msg), buf[n:])
	if err != nil {
		return 0, err
	}

	if len(le.tgl) > 0 {
		n += n1
		n1, err = MarshalString(string(le.tgl), buf[n:])
	} else {
		buf[n] |= byte(128)
	}

	return n + n1, err
}

func (le *LogEvent) Unmarshal(buf []byte) (int, error) {
	n, tgid, err := UnmarshalInt64(buf)
	if err != nil {
		return 0, err
	}
	le.tgid = tgid

	var n1 int
	n1, le.ts, err = UnmarshalInt64(buf[n:])
	if err != nil {
		return 0, err
	}
	n += n1

	lb := buf[n]
	buf[n] &= byte(127)

	n1, le.msg, err = UnmarshalString(buf[n:])
	if err != nil {
		return 0, err
	}
	buf[n] = lb
	n += n1

	if lb&128 == 0 {
		n1, le.tgl, err = UnmarshalString(buf[n:])
		n += n1
	}

	return n, err
}

func (le *LogEvent) String() string {
	return fmt.Sprint("{tGroupId:", le.tgid, ", ts:", uint64(le.ts), ", msg:", le.msg, ", tgl:", le.tgl, "}")
}
