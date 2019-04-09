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

/*
lql package contains parser and helper functions and structures for parsing Lograng Query Language (LQL)
The LQL supports the following constructions:
	SELECT [<format string>] [FROM ({<tags>}|<tags expression)] [WHERE <fields expression>] [POSITION (head|tail|<specific pos>)] [OFFSET <number>][LIMIT <number>]
	SHOW PARTITIONS [({<tags>}|<tags expression)][OFFSET <number>][LIMIT <number>]
	SHOW PIPES [OFFSET <number>][LIMIT <number>]
	DESCRIBE PARTITION {<tags>}
	DESCRIBE PIPE <pipe name>
	TRUNCATE  [({<tags>}|<tags expression)][MINSIZE <size>][MAXSIZE <size>][BEFORE <timestamp>]
	CREATE PIPE <pipe name> [FROM ({<tags>}|<tags expression)] [WHERE <fields expression>]
	DELETE PIPE <pipe name>
*/
package lql
