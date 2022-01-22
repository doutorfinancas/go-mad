package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	var rules Rules

	var testCases = []struct {
		comment string
		config  []byte
		want    Rules
		wantErr bool
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
			false,
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
			false,
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
			false,
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
			false,
		},
		{
			"invalid yaml",
			[]byte("a: 1\nb: 2\na: 3\n"),
			rules,
			true,
		},
	}
	for _, tt := range testCases {
		actual, err := Load(tt.config)
		if (err != nil) != tt.wantErr {
			t.Errorf(
				"TestLoad() error = %v, wantErr %v on %v",
				err,
				tt.wantErr,
				tt.comment,
			)
			continue
		}
		assert.Equal(t, tt.want, actual, tt.comment)
	}
}

func TestRules_RewriteToMap(t *testing.T) {
	type fields struct {
		Rewrite map[string]Rewrite
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]map[string]string
	}{
		{
			"rewrite map test",
			fields{
				map[string]Rewrite{
					"a": {
						"b": "c",
					},
				},
			},
			map[string]map[string]string{
				"a": {
					"b": "c",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				r := Rules{
					Rewrite: tt.fields.Rewrite,
				}
				assert.Equalf(t, tt.want, r.RewriteToMap(), "RewriteToMap()")
			},
		)
	}
}
