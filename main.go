package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/goat-network/dogecoin-relayer/cmd"
)

func main() {
	log.Info("Starting the Dogecoin Relayer program...")
	cmd.Run()
}
