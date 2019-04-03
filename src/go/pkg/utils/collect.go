package utils

func GetKeys(setMap map[string]bool) []string {
	var keys []string
	for key := range setMap {
		keys = append(keys, key)
	}
	return keys
}

func GetKeysFromStringMap(setMap map[string]string) []string {
	var keys []string
	for key := range setMap {
		keys = append(keys, key)
	}
	return keys
}
