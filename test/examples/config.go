package main

import (
	"log"

	"github.com/onlineconf/onlineconf-go/v2"

	"gitlab.cebc.tools/go/tp.git/queueconfig"
)

func getConfig() queueconfig.OnlineConf {
	conf, err := onlineconf.OpenModule("./config.cdb")
	if err != nil {
		log.Fatal(err)
	}

	return queueconfig.NewAdapter(conf)
}
