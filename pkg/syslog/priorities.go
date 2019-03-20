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

package syslog

import "fmt"

type (
	Priority int
)

const (
	SeverityEmerg Priority = iota
	SeverityAlert
	SeverityCrit
	SeverityErr
	SeverityWarning
	SeverityNotice
	SeverityInfo
	SeverityDebug
)

var severities = map[string]Priority{
	"emerg":  SeverityEmerg,
	"alert":  SeverityAlert,
	"crit":   SeverityCrit,
	"err":    SeverityErr,
	"warn":   SeverityWarning,
	"notice": SeverityNotice,
	"info":   SeverityInfo,
	"debug":  SeverityDebug,
}

const (
	FacilityKern = iota << 3
	FacilityUser
	FacilityMail
	FacilityDaemon
	FacilityAuth
	FacilitySyslog
	FacilityLPR
	FacilityNews
	FacilityUUCP
	FacilityCron
	FacilityAuthPriv
	FacilityFTP
	FacilityNTP
	FacilityAudit
	FacilityAlert
	FacilityAt
	FacilityLocal0
	FacilityLocal1
	FacilityLocal2
	FacilityLocal3
	FacilityLocal4
	FacilityLocal5
	FacilityLocal6
	FacilityLocal7
)

var facilities = map[string]Priority{
	"kern":     FacilityKern,
	"user":     FacilityUser,
	"mail":     FacilityMail,
	"daemon":   FacilityDaemon,
	"auth":     FacilityAuth,
	"syslog":   FacilitySyslog,
	"lpr":      FacilityLPR,
	"news":     FacilityNews,
	"uucp":     FacilityUUCP,
	"cron":     FacilityCron,
	"authpriv": FacilityAuthPriv,
	"ftp":      FacilityFTP,
	"ntp":      FacilityNTP,
	"audit":    FacilityAudit,
	"alert":    FacilityAlert,
	"at":       FacilityAt,
	"local0":   FacilityLocal0,
	"local1":   FacilityLocal1,
	"local2":   FacilityLocal2,
	"local3":   FacilityLocal3,
	"local4":   FacilityLocal4,
	"local5":   FacilityLocal5,
	"local6":   FacilityLocal6,
	"local7":   FacilityLocal7,
}

func Severity(n string) (Priority, error) {
	c, ok := severities[n]
	if !ok {
		return -1, fmt.Errorf("unknown severity: %s", n)
	}
	return c, nil
}

func Facility(n string) (Priority, error) {
	c, ok := facilities[n]
	if !ok {
		return -1, fmt.Errorf("unknown facility: %s", n)
	}
	return c, nil
}
