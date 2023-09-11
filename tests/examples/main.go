package main

import (
	"flag"

	zLog "github.com/rs/zerolog/log"
)

var (
	flagID   = flag.Uint("id", 0, "event ID")
	flagName = flag.String("name", "", "event Name")
)

func main() {
	flag.Parse()

	if *flagID == 0 && *flagName == "" {
		startHandler()

		zLog.Info().Msg("handler finished, exiting")
	} else {
		sendData(uint32(*flagID), *flagName)
	}
}
