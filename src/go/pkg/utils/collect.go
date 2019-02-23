package utils

func GetKeys(setMap map[string]bool) []string {
	var keys []string
	for key := range setMap {
		keys = append(keys, key)
	}
	return keys
}
