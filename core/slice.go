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
