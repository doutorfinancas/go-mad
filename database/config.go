package database

import (
	"gopkg.in/yaml.v3"
)

type Rules struct {
	Rewrite map[string]Rewrite `yaml:"rewrite" json:"rewrite"`
	NoData  []string           `yaml:"nodata"  json:"nodata"`
	Ignore  []string           `yaml:"ignore"  json:"ignore"`
	Where   map[string]string  `yaml:"where"   json:"where"`
}

type Rewrite map[string]string

func Load(b []byte) (Rules, error) {
	var rules Rules

	err := yaml.Unmarshal(b, &rules)
	if err != nil {
		return rules, err
	}

	return rules, nil
}
