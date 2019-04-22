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

package xbinary

type (
	// Writable interface is implemented by objects that could be written into io.Writer. The objects that
	// support the interface must provide the size of the object in binary form and the function for
	// writing the object into a Writer.
	Writable interface {
		// WritableSize returns how many bytes the marshalled object form takes
		WritableSize() int

		// WriteTo allows to write (marshal) the object into the writer. If no
		// error happens, the function will write number of bytes returned by the WritableSize() function
		// it returns number of bytes written and an error, if any
		WriteTo(writer *ObjectsWriter) (int, error)
	}

	// WritableString a string which implements Writable
	WritableString string
)

// WritableSize is part of Writable for WritableString
func (ws WritableString) WritableSize() int {
	return WritableStringSize(string(ws))
}

// WriteTo is part of Writable for WritableString
func (ws WritableString) WriteTo(writer *ObjectsWriter) (int, error) {
	return writer.WriteString(string(ws))
}
