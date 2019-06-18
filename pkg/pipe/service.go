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

package pipe

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
	"sort"
	"sync"
)

type (
	// PipesConfig is a configuration struct used for starting the service
	PipesConfig struct {
		// Dir contains the directory where pipes information is persisted
		Dir string

		// Ensure contains slice of pipes to be created on initialization stage
		EnsureAtStart []Pipe
	}

	// Pipe struct describes a pipe.
	Pipe struct {
		// Name contains the pipe name
		Name string
		// TagsCond contains the condition for selecting partitions for the pipe
		TagsCond string
		// FltCond contains the condition for filtering records from the partitions provided.
		FltCond string
	}

	// PipeDesc struct describes a pipe
	PipeDesc struct {
		Pipe

		// DestTags contains the destination tag set.
		DestTags tag.Set
	}

	// Service struct provides functionality by managing pipes
	Service struct {
		CurProvider cursor.Provider   `inject:""`
		Journals    *journal2.Service `inject:""`
		Config      *PipesConfig      `inject:""`

		logger    log4g.Logger
		lock      sync.Mutex
		closedCh  chan struct{}
		closedCtx context.Context
		psr       *persister

		// ppipes maps a pipe name to *ppipe object
		ppipes map[string]*ppipe

		// weCache map of partition name to the list of ppipe for it.
		weCache map[string][]*ppipe

		// wwg is workers watch group
		wwg sync.WaitGroup
	}
)

// NewService creates new pipe Service instance
func NewService() *Service {
	s := new(Service)
	s.logger = log4g.GetLogger("pipe.Service")
	s.ppipes = make(map[string]*ppipe)
	s.closedCh = make(chan struct{})
	s.closedCtx = context2.WrapChannel(s.closedCh)
	s.weCache = make(map[string][]*ppipe)
	return s
}

// Init provides an implementaion of linker.Initializer interface
func (s *Service) Init(ctx context.Context) error {
	err := fileutil.EnsureDirExists(s.Config.Dir)
	if err != nil {
		return errors2.Wrapf(err, "it seems like %s dir doesn't exist, and it is not possible to create it", s.Config.Dir)

	}
	s.psr = newPersister(s.Config.Dir)
	ppipes, err := s.psr.loadPipes()
	if err != nil {
		s.logger.Error("could not read information about pipes. Corrupted? ", err)
		return err
	}

	for _, st := range ppipes {
		stm, err := newPPipe(s, st)
		if err != nil {
			s.logger.Error("Could not create pipe ", st, ", err=", err)
			return err
		}
		s.logger.Debug("creating instance for ", st)
		s.ppipes[st.Name] = stm
	}
	s.logger.Info(len(ppipes), " pipe(s) were created.")

	go s.notificatior()

	err = s.ensurePipesAtStart()
	if err != nil {
		// Shutdown if could not create the pipes
		s.Shutdown()
	}
	return err
}

// Shutdown provides implementation for linker.Shutdowner interface
func (s *Service) Shutdown() {
	close(s.closedCh)
	s.logger.Info("Shutdown(): waiting until all workers done.")
	s.wwg.Wait()
	s.savePipes()

	s.logger.Info("Shutdown(): done with all workers")
}

// EnsurePipe checks whether the pipe exists. It will create the new one if it does not exist.
// The function will return an error if there is a pipe with the name provided, but
// with another settings, than p
func (s *Service) EnsurePipe(p Pipe) (PipeDesc, error) {
	for i := 0; i < 2; i++ {
		p1, err := s.GetPipe(p.Name)
		if err == nil {
			if p1.FltCond != p.FltCond || p1.TagsCond != p.TagsCond {
				return PipeDesc{}, errors2.Errorf("found pipe %s with the same name but different condidions. Requested was %s", p1, p)
			}
			return p1, nil
		}
		_, err = s.CreatePipe(p)
		if err != nil {
			s.logger.Warn("EnsurePipe() p=", p, " create Pipe returned err=", err)
		}
	}
	s.logger.Error("Oops, we either could not get or create the pipe, bug? p=", p)
	return PipeDesc{}, errors2.Errorf("Could not either create and get pipe for %s, seems like corrupted data", p)
}

// CreatePipe creates new pipe by name and conditions provided. Returns an error
// if the pipe already exists or conditions could not be parsed. No error means the pipe
// is created successfully with parameters provided.
func (s *Service) CreatePipe(p Pipe) (PipeDesc, error) {
	s.lock.Lock()
	_, ok := s.ppipes[p.Name]
	s.lock.Unlock()
	if ok {
		return PipeDesc{}, errors2.Errorf("the pipe for name %s, already exists", p.Name)
	}

	stm, err := newPPipe(s, p)
	if err != nil {
		return PipeDesc{}, err
	}

	// check for raise now
	s.lock.Lock()
	_, ok = s.ppipes[p.Name]
	res := PipeDesc{}
	if !ok {
		s.ppipes[p.Name] = stm
		// changing the s.ppipes we need to drop the notification cache as well
		s.weCache = make(map[string][]*ppipe)
		s.logger.Info("New pipe ", p, " has been just created")
		res.Pipe = p
		res.DestTags = stm.tags
	}
	s.lock.Unlock()
	if ok {
		return PipeDesc{}, errors2.Errorf("the pipe for name %s, already exists", p.Name)
	}
	return res, nil
}

// GetPipe returns an existing pipe by its name
func (s *Service) GetPipe(name string) (PipeDesc, error) {
	s.lock.Lock()
	p, ok := s.ppipes[name]
	s.lock.Unlock()
	if ok {
		return PipeDesc{p.getConfig(), p.tags}, nil
	}
	return PipeDesc{}, errors.NotFound
}

// DeletePipe deletes the pipe by its name, but it doesn't delete the corresponding partition. The pipe's
// partition must be deleted via partition.Truncate method
func (s *Service) DeletePipe(name string) error {
	err := errors.NotFound
	s.logger.Info("Deleting pipe ", name)
	s.lock.Lock()
	if p, ok := s.ppipes[name]; ok {
		// changing the s.ppipes we need to drop the notification cache as well
		s.weCache = make(map[string][]*ppipe)
		delete(s.ppipes, name)
		go p.delete()
		err = nil
	} else {
		s.logger.Warn("Pipe with name ", name, " is not found.")
	}
	s.lock.Unlock()
	return err
}

// GetPipes returns the list of known pipes, sorted by name
func (s *Service) GetPipes() []Pipe {
	s.lock.Lock()
	res := make([]Pipe, len(s.ppipes))
	cnt := 0
	for pn, pp := range s.ppipes {
		idx := sort.Search(cnt, func(idx int) bool {
			return res[idx].Name >= pn
		})
		copy(res[idx+1:], res[idx:])
		res[idx] = pp.cfg
	}
	s.lock.Unlock()
	return res
}

func (s *Service) savePipes() {
	s.logger.Info("Saving information about ", len(s.ppipes), " pipes")
	s.lock.Lock()
	ss := make([]Pipe, 0, len(s.ppipes))
	for _, pp := range s.ppipes {
		ss = append(ss, pp.getConfig())
	}
	s.lock.Unlock()
	if err := s.psr.savePipes(ss); err != nil {
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

		pps := s.getPipesForSource(&we)
		for _, pp := range pps {
			pp.onWriteEvent(&we)
		}
	}
}

// getPipesForSource returns list of streams affected by the provided sources. The
// function uses internal cache weCache for fast returning list of streams that correspond
// to the src. If cache is not filled, it will check all partitions against the tags provided and full-fill the cache
func (s *Service) getPipesForSource(we *journal2.WriteEvent) []*ppipe {
	s.lock.Lock()
	if len(s.ppipes) == 0 {
		s.lock.Unlock()
		return nil
	}

	res, ok := s.weCache[we.Src]
	if ok {
		s.lock.Unlock()
		return res
	}

	for _, pp := range s.ppipes {
		if pp.isListeningThePart(we.Tags) {
			if res == nil {
				res = make([]*ppipe, 0, 1)
			}
			res = append(res, pp)
		}
	}
	// to put nil is ok as well
	s.weCache[we.Src] = res
	s.lock.Unlock()
	return res
}

func (s *Service) ensurePipesAtStart() error {
	s.logger.Info("ensurePipesAtStart(): ", s.Config.EnsureAtStart)
	for _, p := range s.Config.EnsureAtStart {
		_, err := s.EnsurePipe(p)
		if err != nil {
			s.logger.Error("ensurePipesAtStart(): error with pipe=", p, ", err=", err)
			return err
		}
	}
	return nil
}

func (p Pipe) String() string {
	return fmt.Sprintf("{Name:%s, TagsCond:%s, FltCond:%s}", p.Name, p.TagsCond, p.FltCond)
}

func (pc *PipesConfig) Apply(other *PipesConfig) {
	if len(other.Dir) > 0 {
		pc.Dir = other.Dir
	}
	if len(other.EnsureAtStart) > 0 {
		pc.EnsureAtStart = other.EnsureAtStart
	}
}

func (pc PipesConfig) String() string {
	return fmt.Sprint("\n\t{\n\t\tDir=", pc.Dir,
		"\n\t\tEnsureAtStart=", pc.EnsureAtStart,
		"\n\t}")
}
