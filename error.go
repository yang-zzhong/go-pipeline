package pipeline

import "strings"

type Error []error

func (errs Error) Error() string {
	strs := []string{}
	for _, e := range errs {
		strs = append(strs, e.Error())
	}
	return strings.Join(strs, ",")
}
