package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	for appId, appk := range cfg.AppId {
		log.Infof("AppId: \"%s\" map to Appk: \"%s\"", appId, appk)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Infof("%s Shutdown...", appName)

	km.Close()
	httpHandler.Stop()

	log.Infof("%s Exit\n---------------\n", appName)
}
