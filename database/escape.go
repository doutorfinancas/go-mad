package database

import (
	"bytes"
	"io"
)

func escape(str string) string {
	var esc string
	var buf bytes.Buffer
	last := 0
	for i, c := range str {
		switch c {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
		default:
			continue
		}
		_, _ = io.WriteString(&buf, str[last:i])
		_, _ = io.WriteString(&buf, esc)
		last = i + 1
	}
	_, _ = io.WriteString(&buf, str[last:])
	return buf.String()
}
