package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendIfMissing(t *testing.T) {
	slice := []string{
		"exists",
	}

	slice = AppendIfNotExists(slice, "exists")
	slice = AppendIfNotExists(slice, "not_exists")

	assert.Equal(t, []string{"exists", "not_exists"}, slice)
}

func TestInSlice(t *testing.T) {
	slice := []string{
		"a", "b", "c", "d",
	}

	assert.True(t, InSlice(slice, "c"))
	assert.False(t, InSlice(slice, "f"))
}
