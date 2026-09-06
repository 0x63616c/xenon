// Package temporal is the version-sensitive embedding boundary.
package temporal

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/temporalstore"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	temporalserver "go.temporal.io/server/temporal"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

//go:embed default.json
var defaults []byte

// Configuration is generated from Xenon settings; upstream config stays private.
func Configuration(c agent.Config) (*config.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	var out config.Config
	if err := yaml.Unmarshal(defaults, &out); err != nil {
		return nil, err
	}
	out.Persistence.NumHistoryShards = c.HistoryShards
	for name, store := range out.Persistence.DataStores {
		store.CustomDataStoreConfig.Options["address"] = c.Address(8)
		out.Persistence.DataStores[name] = store
	}
	out.Global.Membership.BroadcastAddress = c.AdvertiseIP
	for i, name := range []string{"frontend", "history", "matching", "worker"} {
		s := out.Services[name]
		s.RPC.BindOnIP = c.BindIP
		s.RPC.GRPCPort = c.BasePort + i
		s.RPC.MembershipPort = c.BasePort + 4 + i
		if name == "frontend" {
			s.RPC.HTTPPort = c.BasePort + 9
		}
		out.Services[name] = s
	}
	info := out.ClusterMetadata.ClusterInformation["active"]
	info.RPCAddress, info.HTTPAddress = c.PublicAddress, c.PublicHTTPAddress
	out.ClusterMetadata.ClusterInformation = map[string]cluster.ClusterInformation{c.Cluster: info}
	out.ClusterMetadata.CurrentClusterName, out.ClusterMetadata.MasterClusterName = c.Cluster, c.Cluster
	out.PublicClient.HostPort, out.PublicClient.HTTPHostPort = c.PublicAddress, c.PublicHTTPAddress
	return &out, out.Validate()
}

type Runtime struct {
	server     temporalserver.Server
	address    string
	connection *grpc.ClientConn
	config     *config.Config
}

func New(c agent.Config) (*Runtime, error) {
	cfg, err := Configuration(c)
	if err != nil {
		return nil, err
	}
	return &Runtime{config: cfg, address: c.Address(0)}, nil
}

func (r *Runtime) Start(context.Context) error {
	// Fx construction can access persistence. It belongs inside the agent's
	// bounded startup, after storage readiness, rather than in the constructor.
	s, err := temporalserver.NewServer(temporalserver.WithConfig(r.config), temporalserver.WithCustomDataStoreFactory(temporalstore.AbstractFactory{}), temporalserver.WithCustomVisibilityStoreFactory(temporalstore.VisibilityFactory{}), temporalserver.ForServices(temporalserver.DefaultServices))
	if err != nil {
		return err
	}
	r.server = s
	return s.Start()
}
func (r *Runtime) Ready(ctx context.Context) error {
	if r.connection == nil {
		conn, err := grpc.NewClient(r.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		r.connection = conn
	}
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		response, err := grpc_health_v1.NewHealthClient(r.connection).Check(probe, &grpc_health_v1.HealthCheckRequest{Service: "temporal.api.workflowservice.v1.WorkflowService"})
		cancel()
		if err == nil && response.Status == grpc_health_v1.HealthCheckResponse_SERVING {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Temporal API not ready: %w", ctx.Err())
		case <-tick.C:
		}
	}
}
func (r *Runtime) Stop(context.Context) error {
	if r.connection != nil {
		_ = r.connection.Close()
	}
	if r.server != nil {
		return r.server.Stop()
	}
	return nil
}
