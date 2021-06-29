package logging

import (
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
