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
		wantErr bool
	}{
		{
			[]Option{
				OptionValue("set-charset", "utf8mb4"),
				OptionValue("quick", ""),
				OptionValue("single-transaction", ""),
				OptionValue("skip-lock-tables", ""),
			},
			&mySQL{},
			&mySQL{
				quick:             true,
				charset:           "utf8mb4",
				singleTransaction: true,
				lockTables:        false,
			},
			"switch all cases",
			false,
		},
		{
			[]Option{},
			&mySQL{},
			&mySQL{},
			"default",
			false,
		},
		{
			[]Option{
				OptionValue("not-really-an-option", "nor a value"),
			},
			&mySQL{},
			&mySQL{},
			"default",
			true,
		},
	}
	for _, tt := range testCases {
		err := parseMysqlOptions(tt.mysql, tt.options)
		if (err != nil) != tt.wantErr {
			t.Errorf("TestOption() error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		assert.Equal(t, tt.want, tt.mysql, tt.comment)
	}
}
