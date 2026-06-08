package config

import (
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres PostgresConfig `yaml:"postgres"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Pipeline PipelineConfig `yaml:"pipeline"`
	API      APIConfig      `yaml:"api"`
}

type PostgresConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Database        string `yaml:"database"`
	ReplicationSlot string `yaml:"replication_slot"`
	Publication     string `yaml:"publication"`
}

type KafkaConfig struct {
	Brokers     []string `yaml:"brokers"`
	TopicPrefix string   `yaml:"topic_prefix"`
}

type PipelineConfig struct {
	Tables                   []string `yaml:"tables"`
	WalLagWarnThresholdBytes int64    `yaml:"wal_lag_warn_threshold_bytes"`
}

type APIConfig struct {
	Port int `yaml:"port"`
}

// envVarPattern matches ${VAR_NAME} placeholders in the YAML file.
// The inner capture group extracts the variable name between ${ and }.
var envVarPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// Load reads a YAML config file and expands ${ENV_VAR} placeholders
// with values from the environment. Unexpanded placeholders are left as-is
// so that missing variables produce a visible, debuggable value rather than
// silently resolving to an empty string.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Replace ${VAR} with the env var value, or leave unchanged if unset.
	expanded := envVarPattern.ReplaceAllFunc(raw, func(match []byte) []byte {
		// Slice off the leading "${" (2 bytes) and trailing "}" (1 byte).
		key := string(match[2 : len(match)-1])
		if val := os.Getenv(key); val != "" {
			return []byte(val)
		}
		return match
	})
	var cfg Config
	if err := yaml.Unmarshal(expanded, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
