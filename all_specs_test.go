package workers

func setupTestConfig() {
	Configure(map[string]string{
		"server":   "localhost:6379",
		"process":  "1",
		"database": "15",
		"pool":     "1",
	})

	rc := Config.Client
	rc.FlushDB().Result()
}

func setupTestConfigWithNamespace(namespace string) {
	Configure(map[string]string{
		"server":    "localhost:6379",
		"process":   "1",
		"database":  "15",
		"pool":      "1",
		"namespace": namespace,
	})

	rc := Config.Client
	rc.FlushDB().Result()
}
