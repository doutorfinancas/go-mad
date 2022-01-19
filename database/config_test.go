package database

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoad(t *testing.T) {
	var testCases = []struct {
		comment string
		config  []byte
		want    Rules
	}{
		{
			"ignore",
			[]byte(`ignore:
  - table_to_ignore_1
  - table_to_ignore_2`),
			Rules{
				Ignore: []string{
					"table_to_ignore_1",
					"table_to_ignore_2",
				},
			},
		},
		{
			"nodata",
			[]byte(`nodata:
  - table_struct_only_1
  - table_struct_only_2
  - table_struct_only_3`),
			Rules{
				NoData: []string{
					"table_struct_only_1",
					"table_struct_only_2",
					"table_struct_only_3",
				},
			},
		},
		{
			"rewrite",
			[]byte(`rewrite:
  users:
    email: faker.Internet().Email()
    password: '"123"'`),
			Rules{
				Rewrite: map[string]Rewrite{
					"users": map[string]string{
						"email":    "faker.Internet().Email()",
						"password": "\"123\"",
					},
				},
			},
		},
		{
			"mixed",
			[]byte(`
ignore:
  - ignore_table
nodata:
  - structure_only_1
  - structure_only_2
rewrite:
  data_change_table:
    field1: faker.Lorem().Text(100)
where:
  potatoes: |-
    id > 352`),
			Rules{
				Ignore: []string{"ignore_table"},
				NoData: []string{"structure_only_1", "structure_only_2"},
				Rewrite: map[string]Rewrite{
					"data_change_table": map[string]string{
						"field1": "faker.Lorem().Text(100)",
					},
				},
				Where: map[string]string{
					"potatoes": "id > 352",
				},
			},
		},
	}
	for _, testCase := range testCases {
		actual, err := Load(testCase.config)
		assert.Nil(t, err)
		assert.Equal(t, testCase.want, actual, testCase.comment)
	}
}
