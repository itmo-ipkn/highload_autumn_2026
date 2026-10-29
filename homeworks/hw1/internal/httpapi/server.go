package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

func ListenAndServe(ctx context.Context, srv *http.Server, health *Health, shutdownTimeout time.Duration) error {
	if srv.Addr == "" {
		return errors.New("http server address is required")
	}
	listener, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", srv.Addr, err)
	}
	return Serve(ctx, srv, listener, health, shutdownTimeout)
}

func Serve(ctx context.Context, srv *http.Server, listener net.Listener, health *Health, shutdownTimeout time.Duration) error {
	if srv == nil {
		return errors.New("http server is required")
	}
	if listener == nil {
		return errors.New("listener is required")
	}
	if health == nil {
		return errors.New("health is required")
	}
	if shutdownTimeout <= 0 {
		return errors.New("shutdown timeout must be positive")
	}

	health.SetReady(true)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(listener)
	}()

	select {
	case err := <-errCh:
		health.SetReady(false)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		health.SetReady(false)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
