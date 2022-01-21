package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOption(t *testing.T) {
	var testCases = []struct {
		options []Option
		mysql   *mySQL
		want    *mySQL
		comment string
	}{
		{
			[]Option{
				{"set-charset", "utf8mb4"},
				{"quick", ""},
				{"single-transaction", ""},
				{"skip-lock-tables", ""},
			},
			&mySQL{},
			&mySQL{
				quick:             true,
				charset:           "utf8mb4",
				singleTransaction: true,
				lockTables:        false,
			},
			"switch all cases",
		},
		{
			[]Option{},
			&mySQL{},
			&mySQL{},
			"default",
		},
	}
	for _, testCase := range testCases {
		err := parseMysqlOptions(testCase.mysql, testCase.options)
		assert.Nil(t, err)
		assert.Equal(t, testCase.want, testCase.mysql, testCase.comment)
	}
}
