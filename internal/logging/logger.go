package logging

import (
	"os"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
)

func ConfigureLogger() *log.Logger {
	log.SetLevel(log.InfoLevel)
	log.SetReportCaller(false)
	Formatter := new(log.TextFormatter)
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	Formatter.ForceColors = true

	log.SetFormatter(Formatter)
	return log.StandardLogger()
}

func callerPrettyfier(f *runtime.Frame) (string, string) {
	fn := strings.Split(f.Function, " ")[0]
	dir, err := os.Getwd()

	if err != nil {
		dir = f.File
	} else {
		dir = strings.Replace(f.File, dir, "", 1)
	}

	return fn, dir
}
