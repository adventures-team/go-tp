package main

import "github.com/onlineconf/onlineconf-go"

func getConfig() *onlineconf.Module {
	onlineconf.Initialize(".")
	return onlineconf.GetModule("config")
}
