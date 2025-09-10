package main

import (
	"flag"

	zlog "github.com/rs/zerolog/log"
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

		zlog.Info().Msg("handler finished, exiting")
	} else {
		sendData(uint32(*flagID), *flagName)
	}
}
