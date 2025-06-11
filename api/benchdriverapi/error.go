package benchdriverapi

import (
	"encoding/json"
	"net/http"
)

type StatusError struct {
	Code    int
	Err     error
	Display error
}

func (e *StatusError) Error() string {
	return e.Err.Error()
}

func (e *StatusError) Unwrap() error {
	return e.Err
}

func (e *StatusError) StatusCode() int {
	return e.Code
}

func (e *StatusError) DisplayError() error {
	if err := e.Display; err != nil {
		return err
	}
	return e.Err
}

func (e *StatusError) MarhsalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"error": e.DisplayError(),
	})
}

func ErrorBadRequest(err error) *StatusError {
	return &StatusError{http.StatusBadRequest, err, nil}
}

func ErrorBusy(err error) *StatusError {
	return &StatusError{http.StatusConflict, err, nil}
}
