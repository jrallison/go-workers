package workers

import "github.com/sirupsen/logrus"

// WorkersLogger represents the log interface
type WorkersLogger interface {
	Fatal(format ...interface{})
	Fatalf(format string, args ...interface{})
	Fatalln(args ...interface{})

	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Debugln(args ...interface{})

	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Errorln(args ...interface{})

	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Infoln(args ...interface{})

	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Warnln(args ...interface{})
}

// Logger is the default logger
var Logger = initLogger()

func initLogger() WorkersLogger {
	plog := logrus.New()
	plog.Formatter = new(logrus.TextFormatter)
	plog.Level = logrus.InfoLevel
	return plog.WithFields(logrus.Fields{"source": "go-workers"})
}

// SetLogger rewrites the default logger
func SetLogger(l WorkersLogger) {
	if l != nil {
		Logger = l
	}
}
