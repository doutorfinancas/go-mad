package database

import (
	"testing"
)

func TestConfig_ConnectionString(t *testing.T) {
	type fields struct {
		Protocol  string
		Host      string
		Port      string
		Database  string
		User      string
		Pass      string
		Charset   string
		Collation string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"Connection Successful",
			fields{
				"tcp",
				"192.168.55.101",
				"3306",
				"hydra",
				"root",
				"test123",
				"",
				"",
			},
			"root:test123@tcp(192.168.55.101:3306)/hydra?parseTime=true&maxAllowedPacket=0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				Host:     tt.fields.Host,
				Port:     tt.fields.Port,
				Database: tt.fields.Database,
				User:     tt.fields.User,
				Pass:     tt.fields.Pass,
			}
			if got := c.ConnectionString(); got != tt.want {
				t.Errorf("Config.ConnectionString() = %v, want %v", got, tt.want)
			}
		})
	}
}
