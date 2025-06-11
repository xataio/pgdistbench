package server

import (
	"net/http"
)

func getErrorStatusCode(err error) int {
	if se, ok := err.(interface{ StatusCode() int }); ok {
		return se.StatusCode()
	}
	return http.StatusInternalServerError
}

func getDisplayError(err error) error {
	if se, ok := err.(interface{ DisplayError() error }); ok {
		return se.DisplayError()
	}
	return err
}
