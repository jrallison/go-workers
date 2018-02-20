package workers

import (
	"net/url"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval int
	Pool         *redis.Pool
	Fetch        func(queue string) Fetcher
}

var Config *config

func Configure(options map[string]string) {
	var poolSize int
	var namespace string
	var pollInterval int

	if options["server"] == "" {
		panic("Configure requires a 'server' option, which identifies a Redis instance")
	}
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

	poolSize, _ = strconv.Atoi(options["pool"])

	address, err := url.Parse(options["server"])
	if err != nil {
		panic("invalid URL specified for server")
	}
	password := options["password"]
	if address.User != nil {
		if p, ok := address.User.Password(); ok {
			password = p
		}
	}

	Config = &config{
		options["process"],
		namespace,
		pollInterval,
		&redis.Pool{
			MaxIdle:     poolSize,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", address.Host)
				if err != nil {
					return nil, err
				}
				if password != "" {
					if _, err := c.Do("AUTH", password); err != nil {
						c.Close()
						return nil, err
					}
				}
				if options["database"] != "" {
					if _, err := c.Do("SELECT", options["database"]); err != nil {
						c.Close()
						return nil, err
					}
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
		func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}
