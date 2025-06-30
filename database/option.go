package database

import (
	"errors"
	"strconv"
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
		case "hex-encode":
			m.shouldHexBins = true
		case "ignore-generated":
			m.ignoreGenerated = true
		case "dump-trigger":
			m.dumpTrigger = true
		case "skip-definer":
			m.skipDefiner = true
		case "insert-into-limit":
			i, err := strconv.Atoi(v.value)
			if err != nil {
				return err
			}

			m.extendedInsertLimit = i
		case "trigger-delimiter":
			m.triggerDelimiter = v.value
		case "parallel":
			m.parallel = true
		default:
			return errors.New("unknown option")
		}
	}
	return nil
}
