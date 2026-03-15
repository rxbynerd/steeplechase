package receiver

import (
	"compress/gzip"
	"io"
	"net/http"
)

// decompressBody returns a reader for the request body, handling gzip if indicated
// by the Content-Encoding header. The caller must close the returned ReadCloser.
func decompressBody(r *http.Request) (io.ReadCloser, error) {
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		return gz, nil
	}
	return r.Body, nil
}
