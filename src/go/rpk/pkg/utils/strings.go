package utils

func StringInSlice(str string, ss []string) bool {
	for _, s := range ss {
		if str == s {
			return true
		}
	}
	return false
}
