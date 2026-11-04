package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL, Endpoint, AccessKey, SecretKey, Bucket, BackupRoot, RestoreRoot, Token, Listen, Topic string
	Brokers                                                                                            []string
	Secure                                                                                             bool
	ChunkSize, Concurrency, MaxAttempts                                                                int
	Lease, JobTimeout                                                                                  time.Duration
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func Load() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("DATABASE_URL"), Endpoint: env("S3_ENDPOINT", "localhost:9000"), AccessKey: os.Getenv("S3_ACCESS_KEY"), SecretKey: os.Getenv("S3_SECRET_KEY"), Bucket: env("S3_BUCKET", "vaultstore"), BackupRoot: env("BACKUP_ROOT", "./data/sources"), RestoreRoot: env("RESTORE_ROOT", "./data/restores"), Token: os.Getenv("API_TOKEN"), Listen: env("LISTEN_ADDR", ":8080"), Topic: env("KAFKA_TOPIC", "vaultstore.jobs"), Brokers: strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","), Secure: env("S3_SECURE", "false") == "true", Lease: 60 * time.Second, JobTimeout: 6 * time.Hour}
	var err error
	for _, v := range []struct {
		k           string
		d, min, max int
		p           *int
	}{{"CHUNK_SIZE", 4 << 20, 1024, 64 << 20, &c.ChunkSize}, {"WORKER_CONCURRENCY", 2, 1, 32, &c.Concurrency}, {"MAX_ATTEMPTS", 5, 1, 20, &c.MaxAttempts}} {
		*v.p, err = strconv.Atoi(env(v.k, strconv.Itoa(v.d)))
		if err != nil || *v.p < v.min || *v.p > v.max {
			return c, fmt.Errorf("invalid %s", v.k)
		}
	}
	if c.DatabaseURL == "" || c.AccessKey == "" || c.SecretKey == "" || len(c.Token) < 32 {
		return c, fmt.Errorf("DATABASE_URL, S3_ACCESS_KEY, S3_SECRET_KEY and API_TOKEN (32+ characters) required")
	}
	return c, nil
}
