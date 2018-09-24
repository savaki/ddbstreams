package ddbstreams

import "fmt"

type wrappedError struct {
	err error
	msg string
}

func (s wrappedError) Cause() error {
	return s.err
}

func (s wrappedError) Error() string {
	if s.err == nil {
		return s.msg
	}
	return fmt.Sprintf("%v: %v", s.err, s.msg)
}

func wrapErr(err error, format string, args ...interface{}) error {
	return wrappedError{
		err: err,
		msg: fmt.Sprintf(format, args...),
	}
}
