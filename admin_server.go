package theatre

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	mux.HandleFunc("/cluster/actors", as.handleClusterActors)
	mux.HandleFunc("/cluster/actor-detail", as.handleActorDetail)
	mux.HandleFunc("/cluster/schedules", as.handleClusterSchedules)
	mux.HandleFunc("/cluster/types", as.handleClusterTypes)
	mux.HandleFunc("/cluster/all-status", as.handleAllStatus)
	mux.HandleFunc("/cluster/all-schedules", as.handleAllSchedules)
	mux.HandleFunc("/cluster/all-actors", as.handleAllActors)
	mux.HandleFunc("/cluster/actor", as.handleClusterActor)
	mux.HandleFunc("/cluster/local-actor", as.handleLocalActor)
	mux.HandleFunc("/debug/vars", expvar.Handler().ServeHTTP)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/", dashboardHandler(host.config.dashboardDev))

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
	State              string         `json:"state"` // "standalone", "clustered", "frozen", "draining"
	Epoch              int64          `json:"epoch,omitempty"`
	RemainingLeaseMs   int64          `json:"remaining_lease_ms,omitempty"`
	RenewalFailures    int64          `json:"renewal_failures,omitempty"`
	ActiveActors       int            `json:"active_actors"`
	PendingSchedules   int            `json:"pending_schedules"`
	RegisteredTypes    []string       `json:"registered_types"`
	PlacementCacheSize int            `json:"placement_cache_size"`
	Metrics            map[string]int64 `json:"metrics"`

	// Runtime stats.
	Goroutines  int     `json:"goroutines"`
	HeapAllocMB float64 `json:"heap_alloc_mb"`
	HeapSysMB   float64 `json:"heap_sys_mb"`
	GCPauseUs   int64   `json:"gc_pause_us"` // last GC pause in microseconds
	NumGC       int64   `json:"num_gc"`

	// Channel depths (backpressure indicators).
	OutboxDepth int `json:"outbox_depth"`
	OutboxCap   int `json:"outbox_cap"`
	InboxDepth  int `json:"inbox_depth"`
	InboxCap    int `json:"inbox_cap"`

	// Transport stats.
	TransportPeers       int `json:"transport_peers"`
	TransportConnections int `json:"transport_connections"`
	TransportSendQueue   int `json:"transport_send_queue"`
}

func (as *AdminServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h := as.host

	state := "standalone"
	if h.cluster != nil {
		state = "clustered"
	}
	if h.cluster != nil && h.frozen.Load() {
		if h.draining.Load() {
			state = "draining"
		} else {
			state = "frozen"
		}
	}

	// Runtime stats.
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	resp := clusterStatusResponse{
		HostID:           h.hostRef.String(),
		State:            state,
		ActiveActors:     h.actors.Count(),
		PendingSchedules: h.scheduler.count(),
		RegisteredTypes:  h.registeredTypes(),
		Metrics:          h.metrics.Snapshot(),
		Goroutines:       runtime.NumGoroutine(),
		HeapAllocMB:      float64(mem.HeapAlloc) / (1024 * 1024),
		HeapSysMB:        float64(mem.HeapSys) / (1024 * 1024),
		NumGC:            int64(mem.NumGC),
		OutboxDepth:      len(h.outbox),
		OutboxCap:        cap(h.outbox),
		InboxDepth:       len(h.inbox),
		InboxCap:         cap(h.inbox),
	}
	if mem.NumGC > 0 {
		resp.GCPauseUs = int64(mem.PauseNs[(mem.NumGC-1)%256]) / 1000
	}

	if h.transport != nil {
		ts := h.transport.Stats()
		resp.TransportPeers = ts.Peers
		resp.TransportConnections = ts.Connections
		resp.TransportSendQueue = ts.SendQueueDepth
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
	AdminAddr   string `json:"admin_addr,omitempty"`
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
			AdminAddr:   hi.AdminAddr,
			Epoch:       hi.Epoch,
			LeaseExpiry: hi.LeaseExpiry.Format(time.RFC3339),
		}
	}

	writeJSON(w, clusterHostsResponse{Hosts: entries})
}

// actorEntry is a single actor in the GET /cluster/actors response.
type actorEntry struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	Status      string `json:"status"`
	LastMessage string `json:"last_message,omitempty"`
	InboxSize   int    `json:"inbox_size"`
	InboxCap    int    `json:"inbox_cap"`
}

// clusterActorsResponse is the JSON structure for GET /cluster/actors.
type clusterActorsResponse struct {
	Actors []actorEntry `json:"actors"`
}

func (as *AdminServer) handleClusterActors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	actors := as.host.actors.All()
	entries := make([]actorEntry, len(actors))
	for i, a := range actors {
		status := "active"
		if a.GetStatus() != ActorStatusActive {
			status = "inactive"
		}
		e := actorEntry{
			Type:      a.ref.Type,
			ID:        a.ref.ID,
			Status:    status,
			InboxSize: len(a.inbox),
			InboxCap:  cap(a.inbox),
		}
		if lastMsg := a.GetLastMessageTime(); !lastMsg.IsZero() {
			e.LastMessage = lastMsg.Format(time.RFC3339)
		}
		entries[i] = e
	}

	writeJSON(w, clusterActorsResponse{Actors: entries})
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

// actorDetailResponse is the JSON structure for GET /cluster/actor-detail.
type actorDetailResponse struct {
	Type          string `json:"type"`
	ID            string `json:"id"`
	Found         bool   `json:"found"`
	Status        string `json:"status,omitempty"`
	ReceiverType  string `json:"receiver_type,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
	LastMessage   string `json:"last_message,omitempty"`
	UptimeMs      int64  `json:"uptime_ms,omitempty"`
	MessagesTotal int64  `json:"messages_total,omitempty"`
	ErrorsTotal   int64  `json:"errors_total,omitempty"`
	InboxSize     int    `json:"inbox_size,omitempty"`
	InboxCap      int    `json:"inbox_cap,omitempty"`

	// Cluster ownership (if available).
	OwnerHost string `json:"owner_host,omitempty"`
	OwnerAddr string `json:"owner_addr,omitempty"`
	Epoch     int64  `json:"epoch,omitempty"`
}

func (as *AdminServer) handleActorDetail(w http.ResponseWriter, r *http.Request) {
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

	resp := actorDetailResponse{
		Type: actorType,
		ID:   actorID,
	}

	// Local actor info.
	a := h.actors.Lookup(ref)
	if a != nil {
		resp.Found = true
		if a.GetStatus() == ActorStatusActive {
			resp.Status = "active"
		} else {
			resp.Status = "inactive"
		}

		// Receiver type name via reflection.
		rt := reflect.TypeOf(a.receiver)
		if rt.Kind() == reflect.Ptr {
			resp.ReceiverType = fmt.Sprintf("*%s", rt.Elem().Name())
		} else {
			resp.ReceiverType = rt.Name()
		}

		createdAt := atomic.LoadInt64(&a.createdAt)
		if createdAt > 0 {
			resp.CreatedAt = time.Unix(createdAt, 0).Format(time.RFC3339)
			resp.UptimeMs = time.Since(time.Unix(createdAt, 0)).Milliseconds()
		}

		if lastMsg := a.GetLastMessageTime(); !lastMsg.IsZero() {
			resp.LastMessage = lastMsg.Format(time.RFC3339)
		}

		resp.MessagesTotal = atomic.LoadInt64(&a.messagesTotal)
		resp.ErrorsTotal = atomic.LoadInt64(&a.errorsTotal)
		resp.InboxSize = len(a.inbox)
		resp.InboxCap = cap(a.inbox)
	}

	// Cluster ownership info.
	if h.placementCache != nil {
		if entry, ok := h.placementCache.Get(ref); ok {
			resp.OwnerHost = entry.HostID
			resp.OwnerAddr = entry.Address
			resp.Epoch = entry.Epoch
		}
	}
	if resp.OwnerHost == "" && h.cluster != nil && h.cluster.DB() != nil {
		if owner, err := h.resolveOwner(ref); err == nil && owner != nil {
			resp.OwnerHost = owner.HostID
			resp.OwnerAddr = owner.Address
			resp.Epoch = owner.Epoch
		}
	}

	writeJSON(w, resp)
}

// scheduleEntry is a single schedule in the GET /cluster/schedules response.
type scheduleEntry struct {
	ID        int64  `json:"id"`
	ActorType string `json:"actor_type"`
	ActorID   string `json:"actor_id"`
	Body      string `json:"body"`
	Kind      string `json:"kind"` // "one-shot" or "cron"
	CronExpr  string `json:"cron_expr,omitempty"`
	NextFire  string `json:"next_fire"`
}

// clusterSchedulesResponse is the JSON structure for GET /cluster/schedules.
type clusterSchedulesResponse struct {
	Schedules []scheduleEntry `json:"schedules"`
}

func (as *AdminServer) handleClusterSchedules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	infos := as.host.scheduler.list()
	entries := make([]scheduleEntry, len(infos))
	for i, s := range infos {
		kind := "cron"
		if s.OneShot {
			kind = "one-shot"
		}
		entries[i] = scheduleEntry{
			ID:        int64(s.ID),
			ActorType: s.Ref.Type,
			ActorID:   s.Ref.ID,
			Body:      s.Body,
			Kind:      kind,
			CronExpr:  s.CronExpr,
			NextFire:  s.NextFire.Format(time.RFC3339),
		}
	}

	writeJSON(w, clusterSchedulesResponse{Schedules: entries})
}

// clusterTypesResponse is the JSON structure for GET /cluster/types.
type clusterTypesResponse struct {
	Types []string `json:"types"`
}

func (as *AdminServer) handleClusterTypes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, clusterTypesResponse{Types: as.host.registeredTypes()})
}

// --- cluster-wide aggregation ---

// fanOutGet issues parallel HTTP GETs to all remote hosts' admin addresses.
// It returns a map of hostID → response body for reachable hosts.
// The local host is excluded (identified by localHostID).
func (as *AdminServer) fanOutGet(ctx context.Context, path string) map[string][]byte {
	h := as.host
	if h.cluster == nil {
		return nil
	}

	localID := h.localHostID
	if localID == "" {
		localID = h.hostRef.String()
	}

	live := h.cluster.LiveHosts()
	var mu sync.Mutex
	results := make(map[string][]byte)
	var wg sync.WaitGroup

	client := &http.Client{Timeout: 3 * time.Second}

	for _, hi := range live {
		if hi.AdminAddr == "" || hi.HostID == localID {
			continue
		}
		wg.Add(1)
		go func(hi HostInfo) {
			defer wg.Done()
			url := "http://" + hi.AdminAddr + path
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return
			}
			mu.Lock()
			results[hi.HostID] = body
			mu.Unlock()
		}(hi)
	}

	wg.Wait()
	return results
}

// allClusterStatusResponse extends the status response with per-host breakdowns.
type allClusterStatusResponse struct {
	clusterStatusResponse
	Hosts []perHostStatus `json:"hosts,omitempty"`
}

// perHostStatus is a summary of one host's runtime state.
type perHostStatus struct {
	HostID             string           `json:"host_id"`
	State              string           `json:"state"`
	ActiveActors       int              `json:"active_actors"`
	Goroutines         int              `json:"goroutines"`
	HeapAllocMB        float64          `json:"heap_alloc_mb"`
	GCPauseUs          int64            `json:"gc_pause_us"`
	OutboxDepth        int              `json:"outbox_depth"`
	OutboxCap          int              `json:"outbox_cap"`
	InboxDepth         int              `json:"inbox_depth"`
	InboxCap           int              `json:"inbox_cap"`
	TransportPeers       int              `json:"transport_peers"`
	TransportSendQueue   int              `json:"transport_send_queue"`
	PlacementCacheSize   int              `json:"placement_cache_size"`
	Metrics              map[string]int64 `json:"metrics"`
}

func (as *AdminServer) handleAllStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h := as.host

	// Start with local status.
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	snap := h.metrics.Snapshot()
	activeActors := h.actors.Count()
	pendingSchedules := h.scheduler.count()
	goroutines := runtime.NumGoroutine()
	heapAllocMB := float64(mem.HeapAlloc) / (1024 * 1024)
	heapSysMB := float64(mem.HeapSys) / (1024 * 1024)
	numGC := int64(mem.NumGC)
	var gcPauseUs int64
	if mem.NumGC > 0 {
		gcPauseUs = int64(mem.PauseNs[(mem.NumGC-1)%256]) / 1000
	}
	outboxDepth := len(h.outbox)
	outboxCap := cap(h.outbox)
	inboxDepth := len(h.inbox)
	inboxCap := cap(h.inbox)
	var transportPeers, transportConns, transportQueue int
	if h.transport != nil {
		ts := h.transport.Stats()
		transportPeers = ts.Peers
		transportConns = ts.Connections
		transportQueue = ts.SendQueueDepth
	}
	localPlacementCacheSize := 0
	if h.placementCache != nil {
		localPlacementCacheSize = h.placementCache.Len()
	}

	localID := h.localHostID
	if localID == "" {
		localID = h.hostRef.String()
	}

	state := "standalone"
	if h.cluster != nil {
		state = "clustered"
	}

	hosts := []perHostStatus{{
		HostID:             localID,
		State:              state,
		ActiveActors:       activeActors,
		Goroutines:         goroutines,
		HeapAllocMB:        heapAllocMB,
		GCPauseUs:          gcPauseUs,
		OutboxDepth:        outboxDepth,
		OutboxCap:          outboxCap,
		InboxDepth:         inboxDepth,
		InboxCap:           inboxCap,
		TransportPeers:       transportPeers,
		TransportSendQueue:   transportQueue,
		PlacementCacheSize:   localPlacementCacheSize,
		Metrics:              h.metrics.Snapshot(),
	}}

	// Fan-out to all remote hosts' /cluster/status and sum.
	remotes := as.fanOutGet(r.Context(), "/cluster/status")
	for hostID, body := range remotes {
		var remote clusterStatusResponse
		if err := json.Unmarshal(body, &remote); err != nil {
			continue
		}
		activeActors += remote.ActiveActors
		pendingSchedules += remote.PendingSchedules
		goroutines += remote.Goroutines
		heapAllocMB += remote.HeapAllocMB
		heapSysMB += remote.HeapSysMB
		numGC += remote.NumGC
		outboxDepth += remote.OutboxDepth
		outboxCap += remote.OutboxCap
		inboxDepth += remote.InboxDepth
		inboxCap += remote.InboxCap
		transportPeers += remote.TransportPeers
		transportConns += remote.TransportConnections
		transportQueue += remote.TransportSendQueue
		for k, v := range remote.Metrics {
			snap[k] += v
		}
		hosts = append(hosts, perHostStatus{
			HostID:             hostID,
			State:              remote.State,
			ActiveActors:       remote.ActiveActors,
			Goroutines:         remote.Goroutines,
			HeapAllocMB:        remote.HeapAllocMB,
			GCPauseUs:          remote.GCPauseUs,
			OutboxDepth:        remote.OutboxDepth,
			OutboxCap:          remote.OutboxCap,
			InboxDepth:         remote.InboxDepth,
			InboxCap:           remote.InboxCap,
			TransportPeers:     remote.TransportPeers,
			TransportSendQueue: remote.TransportSendQueue,
			PlacementCacheSize: remote.PlacementCacheSize,
			Metrics:            remote.Metrics,
		})
	}

	resp := allClusterStatusResponse{
		clusterStatusResponse: clusterStatusResponse{
			HostID:               h.hostRef.String(),
			State:                state,
			ActiveActors:         activeActors,
			PendingSchedules:     pendingSchedules,
			RegisteredTypes:      h.registeredTypes(),
			Metrics:              snap,
			Goroutines:           goroutines,
			HeapAllocMB:          heapAllocMB,
			HeapSysMB:            heapSysMB,
			GCPauseUs:            gcPauseUs,
			NumGC:                numGC,
			OutboxDepth:          outboxDepth,
			OutboxCap:            outboxCap,
			InboxDepth:           inboxDepth,
			InboxCap:             inboxCap,
			TransportPeers:       transportPeers,
			TransportConnections: transportConns,
			TransportSendQueue:   transportQueue,
		},
		Hosts: hosts,
	}

	// Stable sort by host ID so the frontend doesn't jump around.
	slices.SortFunc(resp.Hosts, func(a, b perHostStatus) int {
		return strings.Compare(a.HostID, b.HostID)
	})

	if h.cluster != nil {
		resp.Epoch = h.cluster.LocalEpoch()
		resp.RemainingLeaseMs = h.cluster.RemainingLease().Milliseconds()
		resp.RenewalFailures = h.cluster.ConsecutiveRenewalFailures()
	}
	// Sum placement cache size from all hosts (local is already in hosts[0]).
	var placementCacheSize int
	for _, ph := range hosts {
		placementCacheSize += ph.PlacementCacheSize
	}
	resp.PlacementCacheSize = placementCacheSize

	writeJSON(w, resp)
}

// allScheduleEntry is like scheduleEntry but always includes host_id.
type allScheduleEntry struct {
	ID        int64  `json:"id"`
	ActorType string `json:"actor_type"`
	ActorID   string `json:"actor_id"`
	Body      string `json:"body"`
	Kind      string `json:"kind"`
	CronExpr  string `json:"cron_expr,omitempty"`
	NextFire  string `json:"next_fire"`
	HostID    string `json:"host_id"`
}

type allSchedulesResponse struct {
	Schedules []allScheduleEntry `json:"schedules"`
}

func (as *AdminServer) handleAllSchedules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h := as.host

	// DB path: query the schedules table directly.
	if h.cluster != nil && h.cluster.DB() != nil {
		rows, err := h.cluster.DB().QueryContext(r.Context(),
			`SELECT schedule_id, actor_type, actor_id, body, cron_expr, next_fire, one_shot, created_by
			 FROM schedules ORDER BY next_fire`)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var entries []allScheduleEntry
		for rows.Next() {
			var (
				id        int64
				actorType string
				actorID   string
				body      []byte
				cronExpr  *string
				nextFire  time.Time
				oneShot   bool
				createdBy string
			)
			if err := rows.Scan(&id, &actorType, &actorID, &body, &cronExpr, &nextFire, &oneShot, &createdBy); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			kind := "cron"
			if oneShot {
				kind = "one-shot"
			}
			e := allScheduleEntry{
				ID:        id,
				ActorType: actorType,
				ActorID:   actorID,
				Body:      fmt.Sprintf("%s", body),
				Kind:      kind,
				NextFire:  nextFire.Format(time.RFC3339),
				HostID:    createdBy,
			}
			if cronExpr != nil {
				e.CronExpr = *cronExpr
			}
			entries = append(entries, e)
		}
		if entries == nil {
			entries = []allScheduleEntry{}
		}
		writeJSON(w, allSchedulesResponse{Schedules: entries})
		return
	}

	// Fan-out path: collect local + remote schedules.
	localID := h.localHostID
	if localID == "" {
		localID = h.hostRef.String()
	}

	// Local schedules.
	infos := h.scheduler.list()
	entries := make([]allScheduleEntry, 0, len(infos))
	for _, s := range infos {
		kind := "cron"
		if s.OneShot {
			kind = "one-shot"
		}
		entries = append(entries, allScheduleEntry{
			ID:        int64(s.ID),
			ActorType: s.Ref.Type,
			ActorID:   s.Ref.ID,
			Body:      s.Body,
			Kind:      kind,
			CronExpr:  s.CronExpr,
			NextFire:  s.NextFire.Format(time.RFC3339),
			HostID:    localID,
		})
	}

	// Remote schedules.
	remotes := as.fanOutGet(r.Context(), "/cluster/schedules")
	for hostID, body := range remotes {
		var resp clusterSchedulesResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			continue
		}
		for _, s := range resp.Schedules {
			entries = append(entries, allScheduleEntry{
				ID:        s.ID,
				ActorType: s.ActorType,
				ActorID:   s.ActorID,
				Body:      s.Body,
				Kind:      s.Kind,
				CronExpr:  s.CronExpr,
				NextFire:  s.NextFire,
				HostID:    hostID,
			})
		}
	}

	writeJSON(w, allSchedulesResponse{Schedules: entries})
}

// allActorEntry is like actorEntry but includes host_id.
type allActorEntry struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	Status      string `json:"status"`
	LastMessage string `json:"last_message,omitempty"`
	InboxSize   int    `json:"inbox_size"`
	InboxCap    int    `json:"inbox_cap"`
	HostID      string `json:"host_id"`
}

type allActorsResponse struct {
	Actors []allActorEntry `json:"actors"`
	Total  int             `json:"total"`
}

func (as *AdminServer) handleAllActors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	limit := 50
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	h := as.host
	localID := h.localHostID
	if localID == "" {
		localID = h.hostRef.String()
	}

	// Local actors.
	actors := h.actors.All()
	entries := make([]allActorEntry, 0, len(actors))
	for _, a := range actors {
		status := "active"
		if a.GetStatus() != ActorStatusActive {
			status = "inactive"
		}
		e := allActorEntry{
			Type:      a.ref.Type,
			ID:        a.ref.ID,
			Status:    status,
			InboxSize: len(a.inbox),
			InboxCap:  cap(a.inbox),
			HostID:    localID,
		}
		if lastMsg := a.GetLastMessageTime(); !lastMsg.IsZero() {
			e.LastMessage = lastMsg.Format(time.RFC3339)
		}
		entries = append(entries, e)
	}

	// Remote actors via fan-out.
	remotes := as.fanOutGet(r.Context(), "/cluster/actors")
	for hostID, body := range remotes {
		var resp clusterActorsResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			continue
		}
		for _, a := range resp.Actors {
			entries = append(entries, allActorEntry{
				Type:        a.Type,
				ID:          a.ID,
				Status:      a.Status,
				LastMessage: a.LastMessage,
				InboxSize:   a.InboxSize,
				InboxCap:    a.InboxCap,
				HostID:      hostID,
			})
		}
	}

	total := len(entries)

	// Sort: newest first (most recent last_message), then by type/id for stability.
	slices.SortFunc(entries, func(a, b allActorEntry) int {
		// Entries without a timestamp sort last.
		switch {
		case a.LastMessage == "" && b.LastMessage == "":
			// fall through to type/id
		case a.LastMessage == "":
			return 1
		case b.LastMessage == "":
			return -1
		default:
			if c := strings.Compare(b.LastMessage, a.LastMessage); c != 0 {
				return c // descending
			}
		}
		if c := strings.Compare(a.HostID, b.HostID); c != 0 {
			return c
		}
		if c := strings.Compare(a.Type, b.Type); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})

	// Paginate.
	if offset > len(entries) {
		offset = len(entries)
	}
	entries = entries[offset:]
	if len(entries) > limit {
		entries = entries[:limit]
	}

	writeJSON(w, allActorsResponse{Actors: entries, Total: total})
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
