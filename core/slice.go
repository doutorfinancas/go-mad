package core

// AppendIfNotExists from the slice.
func AppendIfNotExists(slice []string, add string) []string {
	for _, existing := range slice {
		if existing == add {
			return slice
		}
	}

	return append(slice, add)
}

func InSlice(slice []string, needle string) bool {
	for _, a := range slice {
		if a == needle {
			return true
		}
	}
	return false
}
