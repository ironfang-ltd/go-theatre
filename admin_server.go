package theatre

import (
	"context"
	"encoding/json"
	"expvar"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// AdminServer exposes operational endpoints for a Host over HTTP.
// All responses are JSON. Intended for admin/internal networks only.
type AdminServer struct {
	host     *Host
	server   *http.Server
	listener net.Listener
}

// NewAdminServer creates an AdminServer bound to the given address.
// The server is not started until Start() is called.
func NewAdminServer(host *Host, addr string) (*AdminServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	as := &AdminServer{
		host:     host,
		listener: ln,
		server: &http.Server{
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 60 * time.Second,
		},
	}

	mux.HandleFunc("/cluster/status", as.handleClusterStatus)
	mux.HandleFunc("/cluster/hosts", as.handleClusterHosts)
	mux.HandleFunc("/cluster/actor", as.handleClusterActor)
	mux.HandleFunc("/cluster/local-actor", as.handleLocalActor)
	mux.HandleFunc("/debug/vars", expvar.Handler().ServeHTTP)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return as, nil
}

// Addr returns the listener's address (useful when binding to ":0").
func (as *AdminServer) Addr() string {
	return as.listener.Addr().String()
}

// Start begins serving HTTP requests. Non-blocking.
func (as *AdminServer) Start() {
	go func() {
		if err := as.server.Serve(as.listener); err != nil && err != http.ErrServerClosed {
			slog.Error("admin server error", "error", err)
		}
	}()
	slog.Info("admin server started", "addr", as.Addr())
}

// Stop gracefully shuts down the admin server.
func (as *AdminServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	as.server.Shutdown(ctx)
}

// --- handlers ---

// clusterStatusResponse is the JSON structure for GET /cluster/status.
type clusterStatusResponse struct {
	HostID             string         `json:"host_id"`
	State              string         `json:"state"` // "active", "frozen", "draining", "standalone"
	Epoch              int64          `json:"epoch,omitempty"`
	RemainingLeaseMs   int64          `json:"remaining_lease_ms,omitempty"`
	RenewalFailures    int64          `json:"renewal_failures,omitempty"`
	ActiveActors       int            `json:"active_actors"`
	RegisteredTypes    []string       `json:"registered_types"`
	PlacementCacheSize int            `json:"placement_cache_size"`
	Metrics            map[string]int64 `json:"metrics"`
}

func (as *AdminServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h := as.host

	state := "active"
	if h.cluster == nil {
		state = "standalone"
	} else if h.frozen.Load() {
		if h.draining.Load() {
			state = "draining"
		} else {
			state = "frozen"
		}
	}

	resp := clusterStatusResponse{
		HostID:          h.hostRef.String(),
		State:           state,
		ActiveActors:    h.actors.Count(),
		RegisteredTypes: h.registeredTypes(),
		Metrics:         h.metrics.Snapshot(),
	}

	if h.cluster != nil {
		resp.Epoch = h.cluster.LocalEpoch()
		resp.RemainingLeaseMs = h.cluster.RemainingLease().Milliseconds()
		resp.RenewalFailures = h.cluster.ConsecutiveRenewalFailures()
	}

	if h.placementCache != nil {
		resp.PlacementCacheSize = h.placementCache.Len()
	}

	writeJSON(w, resp)
}

// clusterHostsResponse is the JSON structure for GET /cluster/hosts.
type clusterHostsResponse struct {
	Hosts []hostEntry `json:"hosts"`
}

type hostEntry struct {
	HostID      string `json:"host_id"`
	Address     string `json:"address"`
	Epoch       int64  `json:"epoch"`
	LeaseExpiry string `json:"lease_expiry"`
}

func (as *AdminServer) handleClusterHosts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h := as.host
	if h.cluster == nil {
		writeJSON(w, clusterHostsResponse{Hosts: []hostEntry{}})
		return
	}

	live := h.cluster.LiveHosts()
	entries := make([]hostEntry, len(live))
	for i, hi := range live {
		entries[i] = hostEntry{
			HostID:      hi.HostID,
			Address:     hi.Address,
			Epoch:       hi.Epoch,
			LeaseExpiry: hi.LeaseExpiry.Format(time.RFC3339),
		}
	}

	writeJSON(w, clusterHostsResponse{Hosts: entries})
}

// clusterActorResponse is the JSON structure for GET /cluster/actor.
type clusterActorResponse struct {
	ActorType string `json:"actor_type"`
	ActorID   string `json:"actor_id"`
	OwnerHost string `json:"owner_host,omitempty"`
	Epoch     int64  `json:"epoch,omitempty"`
	Address   string `json:"address,omitempty"`
	Found     bool   `json:"found"`
}

func (as *AdminServer) handleClusterActor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	actorType := r.URL.Query().Get("type")
	actorID := r.URL.Query().Get("id")
	if actorType == "" || actorID == "" {
		http.Error(w, `missing "type" or "id" query parameter`, http.StatusBadRequest)
		return
	}

	h := as.host
	ref := NewRef(actorType, actorID)

	resp := clusterActorResponse{
		ActorType: actorType,
		ActorID:   actorID,
	}

	// Check placement cache first.
	if h.placementCache != nil {
		if entry, ok := h.placementCache.Get(ref); ok {
			resp.OwnerHost = entry.HostID
			resp.Epoch = entry.Epoch
			resp.Address = entry.Address
			resp.Found = true
			writeJSON(w, resp)
			return
		}
	}

	// Fall back to DB lookup.
	if h.cluster != nil && h.cluster.DB() != nil {
		owner, err := h.resolveOwner(ref)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if owner != nil {
			resp.OwnerHost = owner.HostID
			resp.Epoch = owner.Epoch
			resp.Address = owner.Address
			resp.Found = true
		}
	}

	writeJSON(w, resp)
}

// localActorResponse is the JSON structure for GET /cluster/local-actor.
type localActorResponse struct {
	ActorType   string `json:"actor_type"`
	ActorID     string `json:"actor_id"`
	Found       bool   `json:"found"`
	Status      string `json:"status,omitempty"`
	LastMessage string `json:"last_message,omitempty"`
}

func (as *AdminServer) handleLocalActor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	actorType := r.URL.Query().Get("type")
	actorID := r.URL.Query().Get("id")
	if actorType == "" || actorID == "" {
		http.Error(w, `missing "type" or "id" query parameter`, http.StatusBadRequest)
		return
	}

	h := as.host
	ref := NewRef(actorType, actorID)

	resp := localActorResponse{
		ActorType: actorType,
		ActorID:   actorID,
	}

	a := h.actors.Lookup(ref)
	if a != nil {
		resp.Found = true
		if a.GetStatus() == ActorStatusActive {
			resp.Status = "active"
		} else {
			resp.Status = "inactive"
		}
		lastMsg := a.GetLastMessageTime()
		if !lastMsg.IsZero() {
			resp.LastMessage = lastMsg.Format(time.RFC3339)
		}
	}

	writeJSON(w, resp)
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("admin: json encode error", "error", err)
	}
}

// registeredTypes returns the names of all registered actor types.
func (m *Host) registeredTypes() []string {
	var types []string
	m.descriptors.Range(func(key, _ any) bool {
		types = append(types, key.(string))
		return true
	})
	return types
}
