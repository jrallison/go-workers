package workers

import (
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval int
	Client       *redis.Client
	Fetch        func(queue string) Fetcher
}

var Config *config

func Configure(options map[string]string) {
	var namespace string
	var pollInterval int

	redisOptions := &redis.Options{
		IdleTimeout: 240 * time.Second,
	}

	if options["server"] == "" {
		panic("Configure requires a 'server' option, which identifies a Redis instance")
	}
	redisOptions.Addr = options["server"]

	if options["process"] == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}
	if options["pool"] == "" {
		options["pool"] = "1"
	}
	if options["namespace"] != "" {
		namespace = options["namespace"] + ":"
	}
	if seconds, err := strconv.Atoi(options["poll_interval"]); err == nil {
		pollInterval = seconds
	} else {
		pollInterval = 15
	}

	redisOptions.PoolSize, _ = strconv.Atoi(options["pool"])

	if options["database"] != "" {
		dbNum, err := strconv.Atoi(options["database"])
		if err != nil {
			panic("Incorrect database number provided.")
		}
		redisOptions.DB = dbNum
	} else {
		redisOptions.DB = 0
	}

	if options["password"] != "" {
		redisOptions.Password = options["password"]
	}

	Config = &config{
		options["process"],
		namespace,
		pollInterval,
		redis.NewClient(redisOptions),
		func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}
