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
			r.Body.Close()
			return nil, err
		}
		return &gzipReadCloser{gz: gz, body: r.Body}, nil
	}
	return r.Body, nil
}

// gzipReadCloser closes both the gzip reader and the underlying body.
type gzipReadCloser struct {
	gz   *gzip.Reader
	body io.Closer
}

func (g *gzipReadCloser) Read(p []byte) (int, error) {
	return g.gz.Read(p)
}

func (g *gzipReadCloser) Close() error {
	g.gz.Close()
	return g.body.Close()
}
