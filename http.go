package main

import (
	"fmt"
	"io"
	"net/http"
)

func curl(w io.WriteSeeker, u string) error {
	if _, err := w.Seek(0, 0); err != nil {
		return fmt.Errorf("cannot reset to beginning of file: %w", err)
	}
	h, err := http.Get(u)
	if err != nil {
		return fmt.Errorf("failed to execute GET request for %q: %w", u, err)
	}
	defer h.Body.Close()
	if h.StatusCode != 200 {
		return fmt.Errorf("source URL %q returned GET status %03d", u, h.StatusCode)
	}
	nbytes, err := io.Copy(w, h.Body)
	if err != nil {
		return fmt.Errorf("failed while copying contents of %q: %w", u, err)
	}
	if nbytes == 0 {
		return fmt.Errorf("copying contents of %q copied zero bytes", u)
	}
	if h.ContentLength > 0 && nbytes != h.ContentLength {
		return fmt.Errorf("copying contents of %q copied %d bytes but we should have had %d",
			u, nbytes, h.ContentLength)
	}
	return nil
}
