package theatre

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestAdminServer(t *testing.T) (*Host, *AdminServer) {
	t.Helper()

	host := NewHost()
	host.RegisterActor("echo", func() Receiver {
		return &echoReceiver{}
	})
	host.Start()

	as, err := NewAdminServer(host, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewAdminServer: %v", err)
	}
	as.Start()

	return host, as
}

func TestAdmin_ClusterStatus_Standalone(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	resp, err := http.Get("http://" + as.Addr() + "/cluster/status")
	if err != nil {
		t.Fatalf("GET /cluster/status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body clusterStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if body.State != "standalone" {
		t.Errorf("state = %q, want standalone", body.State)
	}
	if body.ActiveActors != 0 {
		t.Errorf("active_actors = %d, want 0", body.ActiveActors)
	}
	if len(body.RegisteredTypes) != 1 || body.RegisteredTypes[0] != "echo" {
		t.Errorf("registered_types = %v, want [echo]", body.RegisteredTypes)
	}
	if body.Metrics == nil {
		t.Error("metrics is nil")
	}
}

func TestAdmin_ClusterStatus_WithActors(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	host.Send(NewRef("echo", "1"), "hi")
	host.Send(NewRef("echo", "2"), "hi")
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://" + as.Addr() + "/cluster/status")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var body clusterStatusResponse
	json.NewDecoder(resp.Body).Decode(&body)

	if body.ActiveActors != 2 {
		t.Errorf("active_actors = %d, want 2", body.ActiveActors)
	}
}

func TestAdmin_ClusterHosts_Standalone(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	resp, err := http.Get("http://" + as.Addr() + "/cluster/hosts")
	if err != nil {
		t.Fatalf("GET /cluster/hosts: %v", err)
	}
	defer resp.Body.Close()

	var body clusterHostsResponse
	json.NewDecoder(resp.Body).Decode(&body)

	if len(body.Hosts) != 0 {
		t.Errorf("hosts = %v, want empty", body.Hosts)
	}
}

func TestAdmin_ClusterActor_MissingParams(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	resp, err := http.Get("http://" + as.Addr() + "/cluster/actor")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestAdmin_ClusterActor_NotFound(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	resp, err := http.Get("http://" + as.Addr() + "/cluster/actor?type=echo&id=999")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var body clusterActorResponse
	json.NewDecoder(resp.Body).Decode(&body)

	if body.Found {
		t.Error("found = true, want false")
	}
	if body.ActorType != "echo" || body.ActorID != "999" {
		t.Errorf("unexpected actor_type/actor_id: %q/%q", body.ActorType, body.ActorID)
	}
}

func TestAdmin_LocalActor_Found(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	host.Send(NewRef("echo", "42"), "hi")
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://" + as.Addr() + "/cluster/local-actor?type=echo&id=42")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var body localActorResponse
	json.NewDecoder(resp.Body).Decode(&body)

	if !body.Found {
		t.Error("found = false, want true")
	}
	if body.Status != "active" {
		t.Errorf("status = %q, want active", body.Status)
	}
}

func TestAdmin_LocalActor_NotFound(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	resp, err := http.Get("http://" + as.Addr() + "/cluster/local-actor?type=echo&id=999")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var body localActorResponse
	json.NewDecoder(resp.Body).Decode(&body)

	if body.Found {
		t.Error("found = true, want false")
	}
}

func TestAdmin_MethodNotAllowed(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	// POST should return 405 for all endpoints.
	endpoints := []string{"/cluster/status", "/cluster/hosts", "/cluster/actor?type=a&id=1", "/cluster/local-actor?type=a&id=1"}
	for _, ep := range endpoints {
		resp, err := http.Post("http://"+as.Addr()+ep, "application/json", nil)
		if err != nil {
			t.Fatalf("POST %s: %v", ep, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 405 {
			t.Errorf("POST %s status = %d, want 405", ep, resp.StatusCode)
		}
	}
}

func TestAdmin_DebugVars(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	resp, err := http.Get("http://" + as.Addr() + "/debug/vars")
	if err != nil {
		t.Fatalf("GET /debug/vars: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("content-type = %q", ct)
	}
}

func TestAdmin_FrozenStatus(t *testing.T) {
	host := NewHost(WithFreezeGracePeriod(50 * time.Millisecond))
	host.RegisterActor("echo", func() Receiver {
		return &echoReceiver{}
	})
	host.Start()
	defer host.Stop()

	as, err := NewAdminServer(host, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewAdminServer: %v", err)
	}
	as.Start()
	defer as.Stop()

	host.Freeze()

	resp, err := http.Get("http://" + as.Addr() + "/cluster/status")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var body clusterStatusResponse
	json.NewDecoder(resp.Body).Decode(&body)

	// Without cluster, frozen host shows state "standalone" because
	// cluster is nil. The frozen check only applies when cluster != nil.
	// In standalone mode, state is always "standalone".
	if body.State != "standalone" {
		t.Errorf("state = %q, want standalone", body.State)
	}
}

// TestAdmin_JSONContentType verifies all endpoints return JSON content-type.
func TestAdmin_JSONContentType(t *testing.T) {
	host, as := newTestAdminServer(t)
	defer host.Stop()
	defer as.Stop()

	endpoints := []string{
		"/cluster/status",
		"/cluster/hosts",
		"/cluster/actor?type=echo&id=1",
		"/cluster/local-actor?type=echo&id=1",
	}

	for _, ep := range endpoints {
		resp, err := http.Get("http://" + as.Addr() + ep)
		if err != nil {
			t.Fatalf("GET %s: %v", ep, err)
		}
		resp.Body.Close()
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("%s Content-Type = %q, want application/json", ep, ct)
		}
	}
}

// TestAdmin_WriteJSON_Handler uses httptest for direct handler testing.
func TestAdmin_WriteJSON_Handler(t *testing.T) {
	host := NewHost()
	host.RegisterActor("echo", func() Receiver {
		return &echoReceiver{}
	})
	host.Start()
	defer host.Stop()

	as := &AdminServer{host: host}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/cluster/status", nil)
	as.handleClusterStatus(rec, req)

	if rec.Code != 200 {
		t.Errorf("status = %d, want 200", rec.Code)
	}

	var body clusterStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.State != "standalone" {
		t.Errorf("state = %q, want standalone", body.State)
	}
}
