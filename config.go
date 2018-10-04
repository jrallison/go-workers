package workers

import (
	"strconv"
	"strings"
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

	if options["process"] == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}

	if options["namespace"] != "" {
		namespace = options["namespace"] + ":"
	}
	if seconds, err := strconv.Atoi(options["poll_interval"]); err == nil {
		pollInterval = seconds
	} else {
		pollInterval = 15
	}

	var redisDB int
	if options["database"] != "" {
		dbNum, err := strconv.Atoi(options["database"])
		if err != nil {
			panic("Incorrect database number provided.")
		}
		redisDB = dbNum
	} else {
		redisDB = 0
	}

	var redisPassword string
	if options["password"] != "" {
		redisPassword = options["password"]
	}

	if options["pool"] == "" {
		options["pool"] = "1"
	}
	redisPoolSize, _ := strconv.Atoi(options["pool"])

	redisIdleTimeout := 240 * time.Second

	var rc *redis.Client
	if options["server"] != "" {
		redisOptions := &redis.Options{
			IdleTimeout: redisIdleTimeout,
			Password:    redisPassword,
			DB:          redisDB,
			PoolSize:    redisPoolSize,
		}

		redisOptions.Addr = options["server"]

		rc = redis.NewClient(redisOptions)
	} else if options["sentinels"] != "" {
		redisOptions := &redis.FailoverOptions{
			IdleTimeout: redisIdleTimeout,
			Password:    redisPassword,
			DB:          redisDB,
			PoolSize:    redisPoolSize,
		}

		redisOptions.SentinelAddrs = strings.Split(options["sentinels"], ",")

		rc = redis.NewFailoverClient(redisOptions)
	} else {
		panic("Configure requires a 'server' or 'sentinels' options, which identify either Redis instance or sentinels.")
	}

	Config = &config{
		processId:    options["process"],
		Namespace:    namespace,
		PollInterval: pollInterval,
		Client:       rc,
		Fetch: func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}
