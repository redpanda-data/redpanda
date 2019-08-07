package utils

func GetKeys(setMap map[string]bool) []string {
	var keys []string
	for key := range setMap {
		keys = append(keys, key)
	}
	return keys
}

func GetIntKeys(setMap map[int]bool) []int {
	var keys []int
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

func ContainsInt(slice []int, elem int) bool {
	for _, n := range slice {
		if elem == n {
			return true
		}
	}
	return false
}
