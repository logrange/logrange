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

package stream

import (
	"context"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/cursor"
	"github.com/logrange/logrange/pkg/model/tag"
	journal2 "github.com/logrange/logrange/pkg/partition"
	context2 "github.com/logrange/range/pkg/context"
	"github.com/logrange/range/pkg/utils/errors"
	"github.com/logrange/range/pkg/utils/fileutil"
	errors2 "github.com/pkg/errors"
	"sync"
)

type (
	// Stream struct describes a stream.
	Stream struct {
		// Name contains the stream name
		Name string
		// TagsCond contains the condition for selecting sources for the stream
		TagsCond string
		// FltCond contains the condition for filtering records from the sources provided.
		FltCond string
	}

	// StreamDesc struct describes a stream
	StreamDesc struct {
		Stream

		// DestTags contains the destination tag set.
		DestTags tag.Set
	}

	// Service struct provides functionality by managing streams
	Service struct {
		CurProvider cursor.Provider   `inject:""`
		Journals    *journal2.Service `inject:""`
		StreamsDir  string            `inject:"streamsDir"`

		logger    log4g.Logger
		lock      sync.Mutex
		closedCh  chan struct{}
		closedCtx context.Context
		sp        *streamPersister

		// strms maps a stream name to *strm object
		strms map[string]*strm

		// weCache map of partition name to the list of streams for it.
		weCache map[string][]*strm

		// wwg is workers watch group
		wwg sync.WaitGroup
	}
)

// NewService creates new stream Service instance
func NewService() *Service {
	s := new(Service)
	s.logger = log4g.GetLogger("stream.Service")
	s.strms = make(map[string]*strm)
	s.closedCh = make(chan struct{})
	s.closedCtx = context2.WrapChannel(s.closedCh)
	s.weCache = make(map[string][]*strm)
	return s
}

// Init provides an implementaion of linker.Initializer interface
func (s *Service) Init(ctx context.Context) error {
	err := fileutil.EnsureDirExists(s.StreamsDir)
	if err != nil {
		return errors2.Wrapf(err, "it seems like %s dir doesn't exist, and it is not possible to create it", s.StreamsDir)

	}
	s.sp = newStreamPersister(s.StreamsDir)
	strms, err := s.sp.loadStreams()
	if err != nil {
		s.logger.Error("could not read information about streams. Currupted? ", err)
		return err
	}

	for _, st := range strms {
		stm, err := newStrm(s, st)
		if err != nil {
			s.logger.Error("Could not create stream ", st, ", err=", err)
			return err
		}
		s.logger.Debug("creating instance for ", st)
		s.strms[st.Name] = stm
	}
	s.logger.Info(len(strms), " stream(s) were created.")

	go s.notificatior()
	return nil
}

// Shutdown provides implementation for linker.Shutdowner interface
func (s *Service) Shutdown() {
	close(s.closedCh)
	s.logger.Info("Shutdown(): waiting until all workers done.")
	s.wwg.Wait()
	s.saveStreams()

	s.logger.Info("Shutdown(): done with all workers")
}

// EnsureStream checks whether the stream exists and it creates the new one if not.
// The function will return an error if there is a stream with the name provided, but
// its conditions are different than in st
func (s *Service) EnsureStream(st Stream) (StreamDesc, error) {
	for i := 0; i < 2; i++ {
		st1, err := s.GetStream(st.Name)
		if err == nil {
			if st1.FltCond != st.FltCond || st1.TagsCond != st.TagsCond {
				return StreamDesc{}, errors2.Errorf("found stream %s with the same name but different condidions. Requested was %s", st1, st)
			}
			return st1, nil
		}
		_, err = s.CreateStream(st)
		if err != nil {
			s.logger.Warn("EnsureStream() st=", st, " create Stream returned err=", err)
		}
	}
	s.logger.Error("Oops, we either could not get or create the stream, bug? st=", st)
	return StreamDesc{}, errors2.Errorf("Could not either create and get stream for %s, seems like corrupted data", st)
}

// CreateStream creates new stream by name and conditions provided. Returns an error
// if the stream already exists or conditions could not be parsed. No error means the stream
// is created successfully with parameters provided.
func (s *Service) CreateStream(st Stream) (StreamDesc, error) {
	s.lock.Lock()
	_, ok := s.strms[st.Name]
	s.lock.Unlock()
	if ok {
		return StreamDesc{}, errors2.Errorf("the stream for name %s, already exists", st.Name)
	}

	stm, err := newStrm(s, st)
	if err != nil {
		return StreamDesc{st, stm.tags}, err
	}

	// check for raise now
	s.lock.Lock()
	_, ok = s.strms[st.Name]
	res := StreamDesc{}
	if !ok {
		s.strms[st.Name] = stm
		// changing the s.strms we need to drop the notification cache as well
		s.weCache = make(map[string][]*strm)
		s.logger.Info("New stream ", st, " has been just created")
		res.Stream = st
		res.DestTags = stm.tags
	}
	s.lock.Unlock()
	if ok {
		return StreamDesc{}, errors2.Errorf("the stream for name %s, already exists", st.Name)
	}
	return res, nil
}

// GetStream returns an existing stream by its name
func (s *Service) GetStream(name string) (StreamDesc, error) {
	s.lock.Lock()
	stm, ok := s.strms[name]
	s.lock.Unlock()
	if ok {
		return StreamDesc{stm.getConfig(), stm.tags}, nil
	}
	return StreamDesc{}, errors.NotFound
}

// DeleteStream delete the stream by its name provided, but it doesn't delete the data. The stream
// data must be deleted via partition.Truncate method
func (s *Service) DeleteStream(name string) error {
	err := errors.NotFound
	s.logger.Info("Deleting stream ", name)
	s.lock.Lock()
	if stm, ok := s.strms[name]; ok {
		// changing the s.strms we need to drop the notification cache as well
		s.weCache = make(map[string][]*strm)
		delete(s.strms, name)
		go stm.delete()
		err = nil
	} else {
		s.logger.Warn("Stream with name ", name, " is not found.")
	}
	s.lock.Unlock()
	return err
}

func (s *Service) saveStreams() {
	s.logger.Info("Saving information about ", len(s.strms), " streams")
	s.lock.Lock()
	ss := make([]Stream, 0, len(s.strms))
	for _, stm := range s.strms {
		ss = append(ss, stm.getConfig())
	}
	s.lock.Unlock()
	if err := s.sp.saveStreams(ss); err != nil {
		s.logger.Error("Could not save infromation about ", len(ss), " streams, err=", err)
	}
}

// notificatior must be run in a separate go-routine. Intends for listening write-events and handle them
func (s *Service) notificatior() {
	s.logger.Info("Entering notificator()")
	defer s.logger.Info("Leaving notificator() ")
	for {
		we, err := s.Journals.GetWriteEvent(s.closedCtx)
		if err != nil {
			s.logger.Info("notificatior(): GetWriteEvent returned err=", err, " considered like closed")
			return
		}

		strms := s.getStreamsForSource(&we)
		for _, stm := range strms {
			stm.onWriteEvent(&we)
		}
	}
}

// getStreamsForSource returns list of streams affected by the provided sours. The
// function uses internal cache weCache for fast returning list of streams that correspond
// to the src. If cache is not filled, it will check all sources against the tags provided and full-fill the cache
func (s *Service) getStreamsForSource(we *journal2.WriteEvent) []*strm {
	s.lock.Lock()
	if len(s.strms) == 0 {
		s.lock.Unlock()
		return nil
	}

	res, ok := s.weCache[we.Src]
	if ok {
		s.lock.Unlock()
		return res
	}

	for _, stm := range s.strms {
		if stm.isListeningTheSource(we.Tags) {
			if res == nil {
				res = make([]*strm, 0, 1)
			}
			res = append(res, stm)
		}
	}
	// to put nil is ok as well
	s.weCache[we.Src] = res
	s.lock.Unlock()
	return res
}

func (st Stream) String() string {
	return fmt.Sprintf("{Name:%s, TagsCond:%s, FltCond:%s}", st.Name, st.TagsCond, st.FltCond)
}
