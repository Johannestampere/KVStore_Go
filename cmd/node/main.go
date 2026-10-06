package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kvstore/internal/api"
	"kvstore/internal/config"
	"kvstore/internal/replication"
	"kvstore/internal/routing"
	"kvstore/internal/storage"
	"kvstore/internal/transport"
)

type nodeOptions struct {
	address        string
	configPath     string
	nodeID         string
	dataDirectory  string
	peerTimeout    time.Duration
	requestTimeout time.Duration
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("node stopped", "error", err)
		os.Exit(1)
	}
}

func run(arguments []string) (runError error) {
	options, err := parseOptions(arguments)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	peerClient, err := transport.NewHTTPNodeClient(options.peerTimeout)
	if err != nil {
		return err
	}
	defer peerClient.CloseIdleConnections()
	handler, drain, err := buildHandler(options, peerClient)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := drain(ctx); err != nil {
			runError = errors.Join(runError, fmt.Errorf("close node services: %w", err))
		}
		if runError == nil {
			slog.Info("node stopped")
		}
	}()

	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", options.address)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", options.address, err)
	}
	router := http.NewServeMux()
	router.Handle("/health", api.HealthHandler{})
	router.Handle("/", handler)
	server := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.Serve(listener)
	}()
	slog.Info("node listening", "address", listener.Addr().String())

	select {
	case err := <-serveErrors:
		return fmt.Errorf("serve HTTP: %w", err)
	case <-shutdownSignal.Done():
		stop() // A second interrupt can terminate a stalled shutdown.
	}

	slog.Info("node shutting down")
	if err := shutdownServer(server); err != nil {
		return err
	}
	if err := <-serveErrors; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	return nil
}

func buildHandler(options nodeOptions, peerClient replication.ReplicaClient) (http.Handler, func(context.Context) error, error) {
	var topology *config.Config
	if options.configPath != "" {
		var err error
		topology, err = config.Load(options.configPath, options.nodeID)
		if err != nil {
			return nil, nil, err
		}
	}
	store, closeStore, err := openStorage(options)
	if err != nil {
		return nil, nil, err
	}
	if topology == nil {
		return api.NewHandler(routing.NewLocalService(store)), func(context.Context) error { return closeStore() }, nil
	}
	handler, drain, err := buildClusterHandler(topology, options, peerClient, store)
	if err != nil {
		return nil, nil, errors.Join(err, closeStore())
	}
	return handler, func(ctx context.Context) error {
		drainError := drain(ctx)
		return errors.Join(drainError, closeStore())
	}, nil
}

type nodeStore interface {
	routing.Store
	replication.Store
}

func openStorage(options nodeOptions) (nodeStore, func() error, error) {
	nodeID := options.nodeID
	if nodeID == "" {
		nodeID = "standalone"
	}
	if options.dataDirectory != "" {
		store, err := storage.OpenPersistentStore(options.dataDirectory, nodeID)
		if err != nil {
			return nil, nil, err
		}
		slog.Info("persistent storage restored", "node_id", nodeID, "directory", options.dataDirectory)
		return store, store.Close, nil
	}
	store, err := storage.NewMemoryStore(nodeID)
	return store, func() error { return nil }, err
}

func buildClusterHandler(topology *config.Config, options nodeOptions, peerClient replication.ReplicaClient, store nodeStore) (http.Handler, func(context.Context) error, error) {
	coordinator, err := replication.NewCoordinator(replication.Options{
		LocalID: topology.Local.ID, Store: store, Membership: topology.Membership,
		Ring: topology.Ring, Client: peerClient,
		ReplicationFactor: topology.ReplicationFactor, Timeout: options.requestTimeout,
		ReadQuorum: topology.ReadQuorum, WriteQuorum: topology.WriteQuorum,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("configure replication: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/internal/", api.NewReplicaHandler(store))
	mux.Handle("/", api.NewHandler(coordinator))
	slog.Info("cluster replication configured", "node_id", topology.Local.ID,
		"advertised_address", topology.Local.Address, "members", len(topology.Membership.NodeIDs()),
		"replicas", topology.ReplicationFactor, "read_quorum", topology.ReadQuorum, "write_quorum", topology.WriteQuorum)
	return mux, coordinator.Shutdown, nil
}

func parseOptions(arguments []string) (nodeOptions, error) {
	var options nodeOptions
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	flags.StringVar(&options.address, "addr", "127.0.0.1:8001", "HTTP listen address (host:port)")
	flags.StringVar(&options.configPath, "config", "", "shared cluster JSON file")
	flags.StringVar(&options.nodeID, "id", "", "local node ID from the cluster configuration")
	flags.StringVar(&options.dataDirectory, "data-dir", "", "node-local log directory (empty uses memory only)")
	flags.DurationVar(&options.peerTimeout, "peer-timeout", 2*time.Second, "maximum duration of a peer request")
	flags.DurationVar(&options.requestTimeout, "request-timeout", 5*time.Second, "maximum duration of a coordinated operation (below 10s)")
	if err := flags.Parse(arguments); err != nil {
		return nodeOptions{}, err
	}
	if flags.NArg() != 0 {
		return nodeOptions{}, errors.New("unexpected positional arguments; use -h for available flags")
	}
	if options.address == "" {
		return nodeOptions{}, errors.New("listen address must not be empty")
	}
	if options.peerTimeout <= 0 {
		return nodeOptions{}, errors.New("-peer-timeout must be positive")
	}
	if options.requestTimeout <= 0 || options.requestTimeout >= 10*time.Second {
		return nodeOptions{}, errors.New("-request-timeout must be positive and below the 10s server write timeout")
	}
	if (options.configPath == "") != (options.nodeID == "") {
		return nodeOptions{}, errors.New("-config and -id must be supplied together")
	}
	return options, nil
}

func shutdownServer(server *http.Server) error {
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownContext); err != nil {
		shutdownError := fmt.Errorf("graceful shutdown: %w", err)
		if closeError := server.Close(); closeError != nil {
			return errors.Join(shutdownError, fmt.Errorf("close server: %w", closeError))
		}
		return shutdownError
	}
	return nil
}
