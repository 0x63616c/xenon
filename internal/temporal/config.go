package temporal

import (
	"fmt"
	"net"
	"strconv"
)

// Config contains only the settings consumed by the upstream embedding boundary.
// Application storage, identity, bootstrap and lifecycle settings belong to app.
type Config struct {
	Cluster, ClusterID                               string
	HistoryShards                                    int32
	BindIP, AdvertiseIP                              string
	BasePort                                         int
	PublicAddress, PublicHTTPAddress, StorageAddress string
}

func (c Config) Validate() error {
	if c.Cluster == "" || len(c.Cluster) > 128 || c.ClusterID == "" {
		return fmt.Errorf("Temporal cluster and durable cluster ID required")
	}
	if net.ParseIP(c.BindIP) == nil || net.ParseIP(c.AdvertiseIP) == nil || net.ParseIP(c.AdvertiseIP).IsUnspecified() {
		return fmt.Errorf("bind_ip and reachable advertise_ip must be IP addresses")
	}
	if c.BasePort < 1024 || c.BasePort > 65525 {
		return fmt.Errorf("base_port must reserve ten ports in 1024..65535")
	}
	if c.HistoryShards < 1 || c.HistoryShards > 16384 {
		return fmt.Errorf("history_shards must be 1..16384")
	}
	for _, address := range []string{c.PublicAddress, c.PublicHTTPAddress, c.StorageAddress} {
		host, port, err := net.SplitHostPort(address)
		number, numberErr := strconv.Atoi(port)
		if err != nil || host == "" || numberErr != nil || number < 1 || number > 65535 {
			return fmt.Errorf("Temporal addresses must be host:port with port 1..65535")
		}
	}
	return nil
}

func (c Config) Address() string {
	return net.JoinHostPort(c.AdvertiseIP, strconv.Itoa(c.BasePort))
}
