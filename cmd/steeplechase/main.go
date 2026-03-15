package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rxbynerd/steeplechase/internal/receiver"
	"github.com/rxbynerd/steeplechase/internal/sink"
)

var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit")
	grpcAddr := flag.String("grpc-addr", ":4317", "gRPC listen address")
	httpAddr := flag.String("http-addr", ":4318", "HTTP listen address")
	flag.Parse()

	if *showVersion {
		fmt.Println("steeplechase", version)
		os.Exit(0)
	}

	s := sink.NewStdoutSink(os.Stdout)

	grpcRecv := receiver.NewGRPCReceiver(*grpcAddr, s)
	httpRecv := receiver.NewHTTPReceiver(*httpAddr, s)

	errCh := make(chan error, 2)

	go func() {
		log.Printf("gRPC receiver listening on %s", *grpcAddr)
		errCh <- grpcRecv.Start()
	}()

	go func() {
		log.Printf("HTTP receiver listening on %s", *httpAddr)
		errCh <- httpRecv.Start()
	}()

	// Wait for interrupt or fatal error
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("Received signal %s, shutting down...", sig)
	case err := <-errCh:
		log.Printf("Receiver error: %v, shutting down...", err)
	}

	// Graceful shutdown with 5s timeout, both servers in parallel
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		grpcRecv.Stop()
	}()
	go func() {
		defer wg.Done()
		if err := httpRecv.Shutdown(ctx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
	}()
	wg.Wait()

	log.Println("Shutdown complete")
}
