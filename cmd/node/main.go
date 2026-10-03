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
	"kvstore/internal/storage"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("node stopped", "error", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	address := flags.String("addr", "127.0.0.1:8001", "HTTP listen address (host:port)")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments; use -addr to set the listen address")
	}
	if *address == "" {
		return errors.New("listen address must not be empty")
	}

	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", *address, err)
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

func shutdownServer(server *http.Server) error {
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(deadline); err != nil {
		shutdownError := fmt.Errorf("graceful shutdown: %w", err)
		if closeError := server.Close(); closeError != nil {
			return errors.Join(shutdownError, fmt.Errorf("close server: %w", closeError))
		}
		return shutdownError
	}
	return nil
}
