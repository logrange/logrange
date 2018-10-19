package atmosphere

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/pkg/dstruct"
	"github.com/logrange/logrange/pkg/util"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/jrivets/log4g"
)

type (
	server struct {
		lock sync.Mutex
		// const - used by multiple go routines
		scfg      ServerConfig
		ctx       context.Context
		ctxCancel context.CancelFunc
		clients   *dstruct.Lru
		sessions  map[int]string
		idCnt     int
		logger    log4g.Logger
	}

	serverClient struct {
		id       int
		lock     sync.Mutex
		cond     *sync.Cond
		state    int
		srv      *server
		strm     stream
		clntRead int
		logger   log4g.Logger
	}
)

const (
	scsWatching = iota
	scsReading
	scsClosed
)

// ------------------------------ server -------------------------------------
func newServer(scfg *ServerConfig) *server {
	s := new(server)
	s.scfg = *scfg
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
	s.logger = log4g.GetLogger("atmosphere.server")
	s.sessions = make(map[int]string)

	// configuring connection timeout
	clnupTimeout := time.Duration(scfg.SessTimeoutMs) * time.Millisecond
	s.clients = dstruct.NewLru(math.MaxInt64, clnupTimeout, s.onDeleteFromCache)
	if clnupTimeout <= 0 {
		clnupTimeout = time.Duration(math.MaxInt64)
	}

	go func() {
		for s.ctx.Err() == nil {
			select {
			case <-s.ctx.Done():
				break
			case <-time.After(clnupTimeout):
				s.connCleanup()
			}
		}

		s.logger.Info("Shutting down, context closed, s.ctx.Err()=", s.ctx.Err())
		s.lock.Lock()
		clnts := make([]*serverClient, 0, s.clients.Len())
		s.clients.Iterate(func(k, v interface{}) bool {
			clnts = append(clnts, v.(*serverClient))
			return true
		})
		s.lock.Unlock()

		// Close all known clients now, without lock
		for _, sc := range clnts {
			sc.Close()
		}
	}()
	return s
}

func (s *server) Serve(conn io.ReadWriteCloser) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Debug("New Connection ", conn)
	if s.ctx.Err() != nil {
		return ErrInvalidState
	}
	s.idCnt++
	sc := newServerClient(s, conn, s.idCnt)
	s.clients.Put(s.idCnt, sc, 1)
	return nil
}

func (s *server) Close() error {
	s.logger.Info("Closing")
	s.ctxCancel()
	return nil
}

// called by serverClient holding its own lock, so never come back for avoiding
// deadlock
func (s *server) onClose(sc *serverClient) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Debug("server client ", sc.id, " closed notification")
	s.clients.DeleteNoCallback(sc.id)
	delete(s.sessions, sc.id)
}

// called by serverClient holding its own lock, so never come back for avoiding
// deadlock
func (s *server) authenticate(sc *serverClient, ar *AuthReq) string {
	ak := strFromPtr(ar.AccessKey)
	sessId := strFromPtr(ar.SessionId)
	if len(sessId) == 0 && !s.scfg.Auth(ak, strFromPtr(ar.SecretKey)) {
		s.logger.Warn("Rejecting ", sc, " wrong authentication request ", &ar)
		return ""
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// this Get() call will refresh position in the LRU cache
	val := s.clients.Get(sc.id)
	if val == nil {
		s.logger.Warn("Rejecting ", sc, " unknown client, may be already closed concurrently? ", ar)
		return ""
	}

	sid, ok := s.sessions[sc.id]
	if len(sessId) > 0 && (!ok || sessId != sid) {
		delete(s.sessions, sc.id)
		s.logger.Warn("Rejecting ", sc, " unknown session, or authentication request ", ar)
		return ""
	}
	sid = util.NewSessionId(48)
	s.sessions[sc.id] = sid
	return sid
}

func (s *server) connCleanup() {
	if s.scfg.SessTimeoutMs <= 0 {
		s.logger.Warn("Session cleanup is turned off, but called. Ignore.")
		return
	}

	s.clients.SweepByTime()
}

// onDeleteFromCache clients Lru callback. Called by container when a client
// is pulled out by session timeout
func (s *server) onDeleteFromCache(k, v interface{}) {
	sc, ok := v.(*serverClient)
	if ok {
		s.logger.Warn("Closing connection by timeout (no heartbeating) ", sc)
		// this notification is called under lock, don't make a deadlock - call
		// the client close in another go-routine
		go sc.Close()
	}
}

// used for test purposes
func (s *server) clientsCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.clients.Len()
}

func (s *server) String() string {
	return fmt.Sprint("{idCnt=", s.idCnt, ", clients=", s.clients.Len(), "}")
}

// --------------------------- serverClient ---------------------------------
func newServerClient(srv *server, rwc io.ReadWriteCloser, id int) *serverClient {
	sc := new(serverClient)
	sc.id = id
	sc.srv = srv
	sc.cond = sync.NewCond(&sc.lock)
	sc.state = scsWatching
	sc.logger = log4g.GetLogger("atmosphere.serverClient").WithId("{" + strconv.Itoa(id) + "}").(log4g.Logger)
	sc.strm.rwc = rwc
	sc.strm.logger = log4g.GetLogger("atmosphere.serverClient.strm").WithId("{" + strconv.Itoa(id) + "}").(log4g.Logger)
	go sc.watcher()
	return sc
}

func (sc *serverClient) String() string {
	return fmt.Sprint("{id=", sc.id, ", state=", sc.state, "}")
}

func (sc *serverClient) Read(buf []byte) (int, error) {
	sc.lock.Lock()
	if sc.state != scsReading {
		sc.logger.Warn("Read is called in state=", sc.state,
			", expecting scsReading(", scsReading, ")")
		return 0, ErrCannotRead
	}
	n := sc.clntRead
	sc.lock.Unlock()

	sc.logger.Trace("Read(): n=", n, ", len(buf)=", len(buf))
	if n > len(buf) {
		n = len(buf)
	}
	n, err := io.ReadFull(sc.strm.rwc, buf[:n])
	sc.clntRead -= n
	if err != nil {
		sc.logger.Warn("Error in a middle of reading client message, err=", err)
		sc.Close()
	}

	return n, err
}

// ReadResponse must be called from the client listener from OnRead() notification
// so it is part of the reading message process.
func (sc *serverClient) ReadResponse(resp []byte) error {
	sc.lock.Lock()
	if sc.state != scsReading {
		sc.logger.Warn("ReadResponse is called in state=", sc.state,
			", expecting scsReading(", scsReading, ")")
		return ErrCannotRead
	}
	skip := sc.clntRead
	sc.clntRead = 0
	sc.lock.Unlock()

	sc.strm.readOrSkip(skip, nil)
	err := sc.strm.writeBuf(ptMessage, resp)

	sc.lock.Lock()
	if sc.state == scsReading {
		sc.state = scsWatching
		sc.cond.Signal()
	}

	if err != nil {
		sc.logger.Warn("Error in wrtiting reading response to client, err=", err)
		sc.close()
	}
	sc.lock.Unlock()
	return err
}

func (sc *serverClient) Close() error {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	return sc.close()
}

func (sc *serverClient) close() error {
	var err error
	if sc.state != scsClosed {
		sc.state = scsClosed
		sc.cond.Broadcast()
		err = sc.strm.close()
		sc.srv.onClose(sc)
	}
	return err
}

func (sc *serverClient) watcher() {
	sc.logger.Info("Starting watcher")
	st := scsWatching
	var sessId string
	for st == scsWatching {
		pt, sz, err := sc.strm.readHeader()
		if err != nil {
			sc.logger.Info("Could not read header (closed?), err=", err)
			sc.Close()
			return
		}

		if pt == ptMessage {
			if len(sessId) == 0 {
				sc.logger.Error("Read request received, but it is not authenticated, close the connection.")
				sc.Close()
				return
			}

			sc.lock.Lock()
			if sc.state == scsWatching {
				sc.state = scsReading
				st = scsReading
				sc.clntRead = sz
			}
			sc.lock.Unlock()

			if st == scsReading {
				err = sc.srv.scfg.ConnListener.OnRead(sc, sz)
				if err != nil {
					sc.logger.Warn("watcher(): listener rejected read, err=", err)
					sc.Close()
					return
				}
			}

			sc.lock.Lock()
			for sc.state == scsReading {
				sc.cond.Wait()
			}
			st = sc.state
			sc.lock.Unlock()
		}

		if pt == ptAuth {
			ar, err := sc.strm.readAuthReq(sz)
			if err != nil {
				sc.logger.Warn("watcher(): could not read auth request, err=", err)
				sc.Close()
				return
			}

			sessId = sc.srv.authenticate(sc, &ar)

			var resp AuthResp
			resp.SessionId = strPtr(sessId)
			resp.Terminal = len(sessId) == 0

			err = sc.strm.writeAuthResp(&resp)
			if err != nil {
				sc.logger.Warn("watcher(): could not send auth response, err=", err)
				sc.Close()
				return
			}

			if len(sessId) == 0 {
				sc.logger.Warn("watcher(): unauthenticated request ", &ar, ", will close it shortly.")
				go func() {
					select {
					case <-sc.srv.ctx.Done():
					case <-time.After(time.Second):
					}
					sc.Close()
				}()
				return
			}
		}
	}
	sc.logger.Info("watcher(): done. state=", st, ", but expecting scs_wathing(", scsWatching, ")")
}
