package config

import (
	"os"
	"strconv"
	"strings"
)

const (
	DefaultNATS     = "nats://localhost:4222"
	DefaultDatabase = "postgres://khan:khan@localhost:5432/khan?sslmode=disable"
	DefaultSymbols  = "ACME-USD"
)

func String(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func Int(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}

func Bool(key string, fallback bool) bool {
	value, err := strconv.ParseBool(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}

func List(key, fallback string) []string {
	var result []string
	for _, item := range strings.Split(String(key, fallback), ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}
