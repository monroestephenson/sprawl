package node

import (
	"fmt"
	"log"

	"github.com/hashicorp/memberlist"
)

type GossipManager struct {
	list   *memberlist.Memberlist
	config *memberlist.Config
}

// NewGossipManager configures a new gossip manager
func NewGossipManager(nodeName, bindAddr string, bindPort int) (*GossipManager, error) {
	cfg := memberlist.DefaultLocalConfig()
	cfg.Name = nodeName
	cfg.BindAddr = bindAddr
	cfg.BindPort = bindPort

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}

	return &GossipManager{
		list:   ml,
		config: cfg,
	}, nil
}

// JoinCluster tries to join the existing cluster via the given seed nodes
func (g *GossipManager) JoinCluster(seeds []string) error {
	if len(seeds) == 0 {
		log.Println("No seeds provided, running as initial node in cluster.")
		return nil
	}

	joined, err := g.list.Join(seeds)
	if err != nil {
		return fmt.Errorf("failed to join cluster: %w", err)
	}
	log.Printf("Joined %d nodes\n", joined)
	return nil
}

// GetMembers returns the current gossip cluster members
func (g *GossipManager) GetMembers() []string {
	members := g.list.Members()
	var names []string
	for _, m := range members {
		names = append(names, fmt.Sprintf("%s (%s:%d)", m.Name, m.Address(), m.Port))
	}
	return names
}

// Shutdown gracefully leaves the cluster
func (g *GossipManager) Shutdown() {
	g.list.Shutdown()
}
