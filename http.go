package main

import (
	"fmt"
	"io"
	"net/http"
)

func curl(w io.Writer, u string) error {
	h, err := http.Get(u)
	if err != nil {
		return fmt.Errorf("failed to execute GET request for %q: %w", u, err)
	}
	defer h.Body.Close()
	if h.StatusCode != 200 {
		return fmt.Errorf("source URL %q returned GET status %03d", u, h.StatusCode)
	}
	_, err = io.Copy(w, h.Body)
	if err != nil {
		return fmt.Errorf("failed while copying contents of %q: %w", u, err)
	}
	return nil
}
