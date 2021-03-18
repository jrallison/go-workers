package workers

import (
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval int
	Redis        redis.UniversalClient
	Fetch        func(queue string) Fetcher
}

var Config *config

func Configure(options map[string]string) {
	var namespace string
	var pollInterval int

	if options["server"] == "" {
		panic("Configure requires a 'server' option, which identifies a Redis instance")
	}
	if options["process"] == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}
	if seconds, err := strconv.Atoi(options["poll_interval"]); err == nil {
		pollInterval = seconds
	} else {
		pollInterval = 15
	}

	namespace = "{worker}:"

	Config = &config{
		options["process"],
		namespace,
		pollInterval,
		newRedisUniversalClient(options["server"]),
		func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}

func newRedisUniversalClient(addr string) redis.UniversalClient {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: strings.Split(addr, ","),
		DB:    0,
	})
	return client
}
