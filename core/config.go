package core

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

// RewriteToMap list for sanitizing the MySQL dump.
func (r Rules) RewriteToMap() map[string]map[string]string {
	selectMap := make(map[string]map[string]string)

	for table, fields := range r.Rewrite {
		selectMap[table] = fields
	}

	return selectMap
}

// WhereToMap list for conditional row exports in the MySQL dump.
func (r Rules) WhereToMap() map[string]string {
	whereMap := make(map[string]string)

	for table, condition := range r.Where {
		whereMap[table] = condition
	}

	return whereMap
}
