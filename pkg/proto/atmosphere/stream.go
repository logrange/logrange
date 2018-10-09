package atmosphere

import (
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/jrivets/log4g"
)

type (
	stream struct {
		rbuf   []byte
		rwc    io.ReadWriteCloser
		logger log4g.Logger
	}
)

func (s *stream) close() error {
	return s.rwc.Close()
}

func (s *stream) readOrSkip(n int, buf []byte) error {
	s.logger.Trace("readOrSkip(): n=", n, ", buf=", len(buf))
	if n > len(buf) {
		for n > 0 {
			idx := len(s.rbuf)
			if idx > n {
				idx = n
			}
			rd, err := s.rwc.Read(s.rbuf[:idx])
			if err != nil {
				s.logger.Debug("readOrSkip(): err=", err)
				return err
			}
			n -= rd
		}
		return nil
	}
	_, err := io.ReadFull(s.rwc, buf[:n])
	return err
}

func (s *stream) authHandshake(ar *AuthReq) (AuthResp, error) {
	err := s.writeAuthReq(ar)
	if err != nil {
		return AuthResp{}, err
	}

	return s.expectingAuthResp()
}

func (s *stream) writeAuthReq(ar *AuthReq) error {
	buf, err := json.Marshal(ar)
	if err != nil {
		s.logger.Warn("writeAuthReq(): could not marshal ar=", ar, ", err=", err)
		return err
	}
	s.logger.Debug("writeAuthReq(): wire buf=", string(buf))

	return s.writeBuf(ptAuth, buf)
}

func (s *stream) writeAuthResp(ar *AuthResp) error {
	buf, err := json.Marshal(ar)
	if err != nil {
		s.logger.Warn("writeAuthResp(): could not marshal ar=", ar, ", err=", err)
		return err
	}
	s.logger.Debug("writeAuthResp(): wire buf=", string(buf))

	return s.writeBuf(ptAuth, buf)
}

func (s *stream) readAuthReq(n int) (AuthReq, error) {
	if len(s.rbuf) < n {
		s.rbuf = make([]byte, n)
	}

	var req AuthReq
	rb := s.rbuf[:n]
	_, err := io.ReadFull(s.rwc, rb)
	if err != nil {
		return req, err
	}

	s.logger.Debug("readAuthReq(): wire buf=", string(rb))
	err = json.Unmarshal(rb, &req)
	return req, err
}

func (s *stream) expectingAuthResp() (AuthResp, error) {
	tp, n, err := s.readHeader()
	if err != nil {
		return AuthResp{}, err
	}
	if tp != ptAuth {
		return AuthResp{}, ErrStreamBroken
	}

	return s.readAuthResp(n)
}

func (s *stream) readAuthResp(n int) (AuthResp, error) {
	if len(s.rbuf) < n {
		s.rbuf = make([]byte, n)
	}

	var resp AuthResp
	rb := s.rbuf[:n]
	_, err := io.ReadFull(s.rwc, rb)
	if err != nil {
		return resp, err
	}

	s.logger.Debug("readAuthResp(): wire buf=", string(rb))
	err = json.Unmarshal(rb, &resp)
	return resp, err
}

func (s *stream) readHeader() (int, int, error) {
	var buf [4]byte
	bs := buf[:]
	_, err := io.ReadFull(s.rwc, bs)
	if err != nil {
		return 0, 0, err
	}
	sz := int(binary.BigEndian.Uint32(bs))

	bs = buf[:2]
	_, err = io.ReadFull(s.rwc, bs)
	if err != nil {
		return 0, 0, err
	}
	typ := int(binary.BigEndian.Uint16(bs))

	s.logger.Trace("readHeader(): sz=", sz, ", typ=", typ)

	return typ, sz, nil
}

func (s *stream) writeBuf(pt int, datBuf []byte) error {
	var buf [4]byte
	bs := buf[:]
	binary.BigEndian.PutUint32(bs, uint32(len(datBuf)))
	_, err := s.rwc.Write(bs)
	if err != nil {
		return err
	}

	bs = buf[:2]
	binary.BigEndian.PutUint16(bs, uint16(pt))
	_, err = s.rwc.Write(bs)
	if err != nil {
		return err
	}
	s.logger.Trace("writeBuf(): sz=", len(datBuf), ", typ=", pt)

	if len(datBuf) > 0 {
		_, err = s.rwc.Write(datBuf)
	}
	return err
}
