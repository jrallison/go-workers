package workers

import (
	"time"

	"github.com/fatih/structs"
	"gopkg.in/redis.v2"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval uint16
	RedisClient  *redis.Client
	Fetch        func(queue string) Fetcher
}

var Config *config

type Options struct {
	Process      string
	Namespace    string
	PollInterval uint16
	RedisOptions *redis.Options
}

func Configure(options *Options) {
	if options.Process == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}

	ops := &Options{
		Namespace:    "",
		PollInterval: 500,
	}

	MergeSimpleStruct(ops, options)

	redisOpts := &redis.Options{
		Network:      "tcp",
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		DialTimeout:  1 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolSize:     20,
		IdleTimeout:  60 * time.Second}

	MergeSimpleStruct(redisOpts, options.RedisOptions)

	client := redis.NewClient(redisOpts)

	Config = &config{
		ops.Process,
		ops.Namespace,
		ops.PollInterval,
		client,
		func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}

func MergeSimpleStruct(dst interface{}, src interface{}) {
	srcM := structs.Map(src)
	srcS := structs.New(src)
	dstS := structs.New(dst)

	for name, value := range srcM {
		if !srcS.Field(name).IsZero() {
			dstS.Field(name).Set(value)
		}
	}
}
