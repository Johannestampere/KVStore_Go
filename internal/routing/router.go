package routing

import (
	"context"
	"errors"
	"fmt"

	"kvstore/internal/cluster"
	"kvstore/internal/consistenthash"
)

// ErrUnavailable indicates that an owner could not be reached.
var ErrUnavailable = errors.New("owner unavailable")

// ErrInvalidResponse indicates that a peer violated the internal protocol.
var ErrInvalidResponse = errors.New("invalid owner response")

// NodeClient performs local-storage operations on a remote node.
type NodeClient interface {
	Put(context.Context, cluster.Member, string, string) error
	Get(context.Context, cluster.Member, string) (string, bool, error)
	Delete(context.Context, cluster.Member, string) error
}

// Options supplies the routing topology and dependencies.
type Options struct {
	LocalID    string
	Store      Store
	Membership *cluster.Membership
	Ring       *consistenthash.Ring
	Client     NodeClient
}

// Router sends operations to one owner without changing replica placement.
type Router struct {
	localID    string
	local      *LocalService
	membership *cluster.Membership
	ring       *consistenthash.Ring
	client     NodeClient
}

// NewRouter validates the topology and connects local and remote access.
func NewRouter(options Options) (*Router, error) {
	if options.Store == nil || options.Membership == nil || options.Ring == nil || options.Client == nil {
		return nil, errors.New("routing requires storage, membership, a ring, and a node client")
	}
	if _, found := options.Membership.Lookup(options.LocalID); !found {
		return nil, fmt.Errorf("local node %q is not a configured member", options.LocalID)
	}
	ids := options.Membership.NodeIDs()
	owners, err := options.Ring.GetNodes("", len(ids))
	if err != nil {
		return nil, fmt.Errorf("routing topology: %w", err)
	}
	if _, err := options.Ring.GetNodes("", len(ids)+1); err == nil {
		return nil, errors.New("ring contains nodes outside membership")
	}
	for _, owner := range owners {
		if _, found := options.Membership.Lookup(owner); !found {
			return nil, fmt.Errorf("ring node %q is not a configured member", owner)
		}
	}
	return &Router{
		localID: options.LocalID, local: NewLocalService(options.Store),
		membership: options.Membership, ring: options.Ring, client: options.Client,
	}, nil
}

// Put writes to the key's assigned owner.
func (router *Router) Put(ctx context.Context, key, value string) error {
	owner, err := router.owner(ctx, key)
	if err != nil {
		return err
	}
	if owner.ID == router.localID {
		return router.local.Put(ctx, key, value)
	}
	return router.client.Put(ctx, owner, key, value)
}

// Get reads from the key's assigned owner.
func (router *Router) Get(ctx context.Context, key string) (string, bool, error) {
	owner, err := router.owner(ctx, key)
	if err != nil {
		return "", false, err
	}
	if owner.ID == router.localID {
		return router.local.Get(ctx, key)
	}
	return router.client.Get(ctx, owner, key)
}

// Delete removes a key on its assigned owner.
func (router *Router) Delete(ctx context.Context, key string) error {
	owner, err := router.owner(ctx, key)
	if err != nil {
		return err
	}
	if owner.ID == router.localID {
		return router.local.Delete(ctx, key)
	}
	return router.client.Delete(ctx, owner, key)
}

func (router *Router) owner(ctx context.Context, key string) (cluster.Member, error) {
	if err := ctx.Err(); err != nil {
		return cluster.Member{}, err
	}
	owners, err := router.ring.GetNodes(key, 1)
	if err != nil {
		return cluster.Member{}, fmt.Errorf("select owner: %w", err)
	}
	member, found := router.membership.Lookup(owners[0])
	if !found {
		return cluster.Member{}, fmt.Errorf("owner %q is not a configured member", owners[0])
	}
	return member, nil
}
