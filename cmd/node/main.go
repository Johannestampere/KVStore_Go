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
	"kvstore/internal/storage"
)

type nodeOptions struct {
	address    string
	configPath string
	nodeID     string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("node stopped", "error", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	options, err := parseOptions(arguments)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if options.configPath != "" {
		topology, err := config.Load(options.configPath, options.nodeID)
		if err != nil {
			return err
		}
		slog.Info("cluster configuration loaded", "node_id", topology.Local.ID,
			"advertised_address", topology.Local.Address, "members", len(topology.Membership.NodeIDs()))
	}

	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", options.address)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", options.address, err)
	}
	server := &http.Server{
		Handler:           api.NewHandler(storage.NewMemoryStore()),
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
	slog.Info("node stopped")
	return nil
}

func parseOptions(arguments []string) (nodeOptions, error) {
	var options nodeOptions
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	flags.StringVar(&options.address, "addr", "127.0.0.1:8001", "HTTP listen address (host:port)")
	flags.StringVar(&options.configPath, "config", "", "shared cluster JSON file")
	flags.StringVar(&options.nodeID, "id", "", "local node ID from the cluster configuration")
	if err := flags.Parse(arguments); err != nil {
		return nodeOptions{}, err
	}
	if flags.NArg() != 0 {
		return nodeOptions{}, errors.New("unexpected positional arguments; use -h for available flags")
	}
	if options.address == "" {
		return nodeOptions{}, errors.New("listen address must not be empty")
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
