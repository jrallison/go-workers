package workers

import "github.com/garyburd/redigo/redis"

func queueKey(simpleName string) string {
	return Config.Namespace + "queue:" + simpleName
}

func scheduledQueueKey() string {
	return Config.Namespace + SCHEDULED_JOBS_KEY
}

// QueueSize returns the current size of the queue with the given name.
func QueueSize(queue string) (int, error) {
	conn := Config.Pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("llen", queueKey(queue)))
}

// ScheduledQueueSize returns the number of jobs in the scheduled jobs queue.
func ScheduledQueueSize() (int, error) {
	conn := Config.Pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("zcard", scheduledQueueKey()))
}
