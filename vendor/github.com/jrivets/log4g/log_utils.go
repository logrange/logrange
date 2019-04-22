package log4g

import (
	"regexp"
	"strings"

	"github.com/jrivets/gorivets"
)

const maxInt64 = 1<<63 - 1

type logNameProvider interface {
	name() string
}

func compare(n1, n2 logNameProvider) int {
	switch {
	case n1.name() == n2.name():
		return 0
	case n1.name() < n2.name():
		return -1
	}
	return 1
}

// loggerName cannot start/end from spaces and dots
func normalizeLogName(name string) string {
	return strings.Trim(name, ". ")
}

/**
 * Checks whether the checkedName is ancestor for the loggerName or not
 * The name checkedName is ancestor for the loggerName if:
 *	- checkedName == loggerName
 *  - loggerName == checkedName.<some name here>
 * 	- checkedName == rootLoggerName
 */
func ancestor(checkedName, loggerName string) bool {
	if checkedName == loggerName || checkedName == rootLoggerName {
		return true
	}

	lenc := len(checkedName)
	lenl := len(loggerName)
	if strings.HasPrefix(loggerName, checkedName) && lenl > lenc && loggerName[lenc] == '.' {
		return true
	}
	return false
}

func getNearestAncestor(comparable gorivets.Comparable, names *gorivets.SortedSlice) logNameProvider {
	if names.Len() == 0 {
		return nil
	}
	nProvider := comparable.(logNameProvider)
	for idx := gorivets.Min(names.Len()-1, names.GetInsertPos(nProvider.(gorivets.Comparable))); idx >= 0; idx-- {
		candidate := names.At(idx).(logNameProvider)
		if ancestor(candidate.name(), nProvider.name()) {
			return candidate
		}
	}
	return nil
}

// Gets the name of a parameter provided in the form: <prefix>.<name>.<attribute>
func getConfigParamName(param, prefix string, checker func(string) bool) (string, bool) {
	pr := prefix + "."
	if !strings.HasPrefix(param, pr) {
		return "", false
	}

	start := len(pr)
	end := strings.LastIndex(param, ".")
	if start == end+1 {
		return "", true
	}

	paramName := param[start:end]
	if checker != nil && !checker(paramName) {
		panic("Unacceptable param value \"" + paramName + "\" for " + prefix + " setting.")
	}

	return paramName, true
}

// Gets the attribute of a parameter provided in the form: <prefix>.<name>.<attribute>
func getConfigParamAttribute(param string) string {
	end := strings.LastIndex(param, ".")
	if end == len(param)-1 {
		return ""
	}
	return param[end+1:]
}

// Groups params with the prefix by their names into a map of maps, where the second
// map defines parameters for the particular key value (param name) from the first map
func groupConfigParams(params map[string]string, prefix string, checker func(string) bool) map[string]map[string]string {
	resultMap := make(map[string]map[string]string)
	for k, v := range params {
		name, ok := getConfigParamName(k, prefix, checker)
		if !ok {
			continue
		}
		attribute := getConfigParamAttribute(k)

		m, ok := resultMap[name]
		if !ok {
			m = make(map[string]string)
			resultMap[name] = m
		}
		m[attribute] = v
	}
	return resultMap
}

func isCorrectAppenderName(appenderName string) bool {
	matched, err := regexp.MatchString("^[A-Za-z][A-Za-z0-9.]+$", appenderName)
	if !matched || err != nil {
		return false
	}
	return true
}

func isCorrectLoggerName(loggerName string) bool {
	if loggerName == "" {
		return true
	}
	matched, err := regexp.MatchString("^[A-Za-z]+([A-Za-z0-9.-_]*[A-Za-z0-9]+)*$", loggerName)
	if !matched || err != nil {
		return false
	}
	return true
}
