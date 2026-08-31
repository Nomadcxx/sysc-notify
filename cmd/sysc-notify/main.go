package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nomadcxx/sysc-notify/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, app.Config{}); err != nil {
		log.Printf("sysc-notify: %v", err)
		os.Exit(1)
	}
}
