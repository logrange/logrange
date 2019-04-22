package gorivets

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Global Map is a key-value storage which can be solely present in a process.
// The idea behind this it is to have a singleton storage. Such storage would
// allow, for instance, to register an information from a programming component
// and to support the component singularity itself. For instance, with introducing
// a "vendoring" feature the idea of having singleton implementation, which is
// based on global variables, becomes miserable, because the same code can easily
// appear in different (vendored) packages of different the project libraries.
// To introduce a run-time check of incorrectness of the project, the Global Map
// can be used.

// The implementation is based on creating a temporary file which will be
// deleted every time when the map becomes empty. Also the GMapCleanup() allows
// to clean the storage and remove the temporary file unconditionally.

// Global Map uses the temporary file to control its own copies and panics if
// the expected file already exists (what indicates that another copy of Global
// Map already created it).

// The temporary file has PID in its name, what allows to have different instances
// of the porcess to run simultaneously.
var gmap struct {
	gm   map[string]interface{}
	file *os.File
	lock sync.Mutex
}

// Returns a value by key. It returns whether the key-value pair is in the
// storage in the second bool parameter
func GMapGet(key string) (interface{}, bool) {
	gmap.lock.Lock()
	defer gmap.lock.Unlock()

	if !checkGMap(false) {
		return nil, false
	}

	v, ok := gmap.gm[key]
	return v, ok
}

// Puts a value by key. It returns previously stored value if it was there.
// Second returned parameter indicates whether there was a value for the key
// before the call
func GMapPut(key string, value interface{}) (interface{}, bool) {
	gmap.lock.Lock()
	defer gmap.lock.Unlock()

	checkGMap(true)
	v, ok := gmap.gm[key]
	gmap.gm[key] = value
	return v, ok
}

// Deletes a value by its key. If the storage becomes empty the deleteGMap()
// will be called
func GMapDelete(key string) interface{} {
	gmap.lock.Lock()
	defer gmap.lock.Unlock()

	if !checkGMap(false) {
		return nil
	}

	v := gmap.gm[key]
	delete(gmap.gm, key)

	if len(gmap.gm) == 0 {
		deleteGMap()
	}

	return v
}

// Cleans up the storage and removes the temporary file if it exists.
// All consequential calls of GMapPut() will cause of re-initilization of the
// Global Map what will require to call the GMapCleanup() once again.
func GMapCleanup() {
	gmap.lock.Lock()
	defer gmap.lock.Unlock()

	deleteGMap()
}

func deleteGMap() {
	if gmap.file != nil {
		fn := gmap.file.Name()
		gmap.file.Close()
		os.Remove(fn)
		gmap.file = nil
		gmap.gm = nil
	}
}

func checkGMap(create bool) bool {
	if gmap.gm != nil {
		return true
	}

	if !create {
		return false
	}

	createGMapFile()
	gmap.gm = make(map[string]interface{})
	return true
}

func createGMapFile() {
	dir := os.TempDir()
	fileName := fmt.Sprintf("gorivets.globalMap.%d", os.Getpid())
	name := filepath.Join(dir, fileName)
	var err error
	gmap.file, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		panic(errors.New("\nCould not create global map: the file \"" +
			name + "\" already exists. It could happen by one of the following reasons:\n" +
			"\t1) different versions of gorivets are linked into the application - check vendors folders of the application dependencies\n" +
			"\t2) the file left from a previously run process, which has been crashed - delete it and re-run the application.\n"))
	}
}
