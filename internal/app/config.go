package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/0x63616c/xenon/internal/temporal"
)

// TemporalConfig validates the full application configuration before projecting
// the immutable values consumed by Temporal. CLI validation and Run share this seam.
func (c Config) TemporalConfig() (temporal.Config, error) {
	clusterID := c.Cluster
	if c.ServiceStorage != nil {
		copy := c.ServiceStorage.Clone()
		c.ServiceStorage = &copy
		clusterID = string(c.ServiceStorage.ClusterID)
	}
	if err := c.Validate(); err != nil {
		return temporal.Config{}, err
	}
	return temporal.Config{Cluster: c.Cluster, ClusterID: clusterID, HistoryShards: c.HistoryShards,
		BindIP: c.BindIP, AdvertiseIP: c.AdvertiseIP, BasePort: c.BasePort,
		PublicAddress: c.PublicAddress, PublicHTTPAddress: c.PublicHTTPAddress,
		StorageAddress: c.Address(8)}, nil
}

// Config contains customer configuration only. Credentials remain external.
// Ports BasePort..BasePort+9 must be reachable between agents on a trusted network.
type Config struct {
	Cluster            string `json:"cluster"`
	Node               string `json:"node"`
	Bucket             string `json:"bucket"`
	Prefix             string `json:"prefix"`
	BindIP             string `json:"bind_ip"`
	AdvertiseIP        string `json:"advertise_ip"`
	BasePort           int    `json:"base_port"`
	PublicAddress      string `json:"public_address"`
	PublicHTTPAddress  string `json:"public_http_address"`
	DiagnosticsAddress string `json:"diagnostics_address,omitempty"`
	HistoryShards      int32  `json:"history_shards"`
	// Bootstrap is explicit: an empty/mistyped prefix must not silently create a cluster.
	Bootstrap      bool                  `json:"bootstrap"`
	ServiceStorage *ServiceStorageConfig `json:"service_storage,omitempty"`
}

func Load(path string) (Config, error) {
	var c Config
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil {
		return c, err
	}
	if len(data) > 65536 {
		return c, fmt.Errorf("oversized agent configuration")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) == nil {
		var storage map[string]json.RawMessage
		if json.Unmarshal(fields["service_storage"], &storage) == nil {
			if value, exists := storage["wal_flush_interval_ms"]; exists && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return c, fmt.Errorf("wal_flush_interval_ms must not be null")
			}
		}
		if value, exists := fields["service_storage"]; exists && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return c, fmt.Errorf("service_storage must be an explicit configuration, not null")
		}
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return c, err
	}
	if d.Decode(new(any)) != io.EOF {
		return c, fmt.Errorf("trailing or oversized agent configuration")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.Cluster == "" || len(c.Cluster) > 128 || c.Node == "" || len(c.Node) > 128 || c.Bucket == "" {
		return fmt.Errorf("cluster, node and bucket required")
	}
	if c.Prefix == "" || strings.ContainsAny(c.Prefix, "\\\x00") {
		return fmt.Errorf("invalid S3 prefix")
	}
	for _, p := range strings.Split(c.Prefix, "/") {
		if p == "" || p == "." || p == ".." {
			return fmt.Errorf("invalid S3 prefix")
		}
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
	for _, address := range []string{c.PublicAddress, c.PublicHTTPAddress} {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" || port == "" {
			return fmt.Errorf("public addresses must be host:port")
		}
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return fmt.Errorf("public address ports must be 1..65535")
		}
	}
	if c.DiagnosticsAddress != "" {
		host, port, err := net.SplitHostPort(c.DiagnosticsAddress)
		number, numberErr := strconv.Atoi(port)
		if err != nil || host == "" || numberErr != nil || number < 1 || number > 65535 {
			return fmt.Errorf("diagnostics_address must be host:port with port 1..65535")
		}
	}
	if c.ServiceStorage != nil {
		if !utf8.ValidString(c.Cluster) || strings.TrimSpace(c.Cluster) != c.Cluster || strings.IndexFunc(c.Cluster, unicode.IsControl) >= 0 {
			return fmt.Errorf("invalid cluster manifest name")
		}
		if len(c.Prefix+"/metadata/registry/cluster/control") > 1024 {
			return fmt.Errorf("service namespace exceeds S3 key limit")
		}
		return c.ServiceStorage.Validate(c.Prefix)
	}
	return nil
}

func (c Config) Address(offset int) string {
	return net.JoinHostPort(c.AdvertiseIP, fmt.Sprint(c.BasePort+offset))
}
func (c Config) Listen(offset int) string {
	return net.JoinHostPort(c.BindIP, fmt.Sprint(c.BasePort+offset))
}
