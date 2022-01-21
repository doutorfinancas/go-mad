package database

import (
	"errors"
)

type Option struct {
	key   string
	value string
}

func OptionValue(key, value string) Option {
	return Option{key: key, value: value}
}

func parseMysqlOptions(m *mySQL, options []Option) error {
	for _, v := range options {
		switch v.key {
		case "set-charset":
			m.charset = v.value
		case "quick":
			m.quick = true
		case "single-transaction":
			m.singleTransaction = true
		case "skip-lock-tables":
			m.lockTables = false
		default:
			return errors.New("unknown option")
		}
	}
	return nil
}
