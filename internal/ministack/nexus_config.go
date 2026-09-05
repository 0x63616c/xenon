package ministack

import (
	"fmt"
	"go.temporal.io/server/common/config"
	"net"
)

// CheckNexusConfig runs the pinned server loader/validator before checking the
// additional HTTP requirements for the declared multiple-process ministack.
func CheckNexusConfig(path string) error {
	c, err := config.Load(config.WithConfigFile(path))
	if err != nil {
		return err
	}
	frontend, ok := c.Services["frontend"]
	if !ok || frontend.RPC.HTTPPort < 1 || frontend.RPC.HTTPPort > 65535 {
		return fmt.Errorf("frontend HTTP listener required for Nexus")
	}
	for _, service := range c.Services {
		if service.RPC.GRPCPort == frontend.RPC.HTTPPort {
			return fmt.Errorf("HTTP listener collides with gRPC port")
		}
	}
	if c.ClusterMetadata == nil {
		return fmt.Errorf("cluster metadata required")
	}
	current, ok := c.ClusterMetadata.ClusterInformation[c.ClusterMetadata.CurrentClusterName]
	if !ok || current.HTTPAddress == "" {
		return fmt.Errorf("cluster HTTP address required")
	}
	if _, _, err = net.SplitHostPort(current.HTTPAddress); err != nil {
		return fmt.Errorf("cluster HTTP address must be host:port")
	}
	if c.PublicClient.HTTPHostPort != current.HTTPAddress {
		return fmt.Errorf("public HTTP client must use the same stable cluster ingress")
	}
	return nil
}
