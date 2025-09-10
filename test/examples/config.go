package main

import (
	"log"

	"github.com/onlineconf/onlineconf-go/v2"
)

func getConfig() *onlineconf.Module {
	conf, err := onlineconf.OpenModule("./config.cdb")
	if err != nil {
		log.Fatal(err)
	}

	return conf
}
