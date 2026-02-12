package theatre

import (
	"embed"
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
)

//go:embed all:web/dashboard/dist
var dashboardDist embed.FS

// dashboardHandler returns an http.Handler that serves the React dashboard.
// In dev mode it reverse-proxies to the Vite dev server on localhost:3000.
// In production it serves the embedded SPA from the dist/ directory, falling
// back to index.html for client-side routing.
func dashboardHandler(devProxy bool) http.Handler {
	if devProxy {
		target, _ := url.Parse("http://localhost:3000")
		return httputil.NewSingleHostReverseProxy(target)
	}

	dist, _ := fs.Sub(dashboardDist, "web/dashboard/dist")
	return spaHandler{fs: http.FS(dist)}
}

// spaHandler serves static files from an http.FileSystem and falls back to
// index.html for paths that don't match a file (client-side routing support).
type spaHandler struct {
	fs http.FileSystem
}

func (h spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// Try to open the requested file.
	f, err := h.fs.Open(path)
	if err != nil {
		// File not found — serve index.html for SPA routing.
		r.URL.Path = "/"
		http.FileServer(h.fs).ServeHTTP(w, r)
		return
	}
	f.Close()

	http.FileServer(h.fs).ServeHTTP(w, r)
}
