package theatre

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// hostSeq provides a unique per-process instance number so multiple hosts
// created in the same second get distinct HostRefs.
var hostSeq atomic.Int64

type HostRef struct {
	IPAddress string
	Port      int
	Epoch     int
}

func NewHostRef(ip string, port int, epoch int) HostRef {
	return HostRef{
		IPAddress: ip,
		Port:      port,
		Epoch:     epoch,
	}
}

func (r HostRef) String() string {
	return fmt.Sprintf("%s:%d:%d", r.IPAddress, r.Port, r.Epoch)
}

func createNewHostRef() (HostRef, error) {

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return HostRef{}, err
	}

	seq := hostSeq.Add(1)
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				epoch := int(time.Now().Unix())
				return NewHostRef(ipnet.IP.String(), 6969+int(seq)-1, epoch), nil
			}
		}
	}

	return HostRef{}, fmt.Errorf("no suitable IP address found")
}
