package database

type Option struct {
	key   string
	value string
}

func OptionValue(key string, value string) Option {
	return Option{key: key, value: value}
}

func parseMysqlOptions(m *mySql, options []Option) error {
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
		}
	}
	return nil
}
