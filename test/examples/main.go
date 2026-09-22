package main

import (
	"flag"
	"log/slog"
)

var (
	flagID   = flag.Uint("id", 0, "UserID")
	flagName = flag.String("name", "", "UserName")
)

func main() {
	flag.Parse()

	svc := &Service{DatabaseConnection: "test connection"}

	if *flagID == 0 && *flagName == "" {
		startHandler(svc)

		slog.Info("handler finished, exiting")
	} else {
		sendData(uint32(*flagID), *flagName)
	}
}
