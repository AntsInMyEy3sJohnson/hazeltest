package config

import (
	"fmt"
	"strings"
)

func ExtractConfigValue(sourceMap map[string]interface{}, keyPath string) (interface{}, error) {

	pathElements := strings.Split(keyPath, ".")

	if len(pathElements) == 1 {
		return sourceMap[keyPath], nil
	}

	currentPathElement := pathElements[0]
	sourceMap, ok := sourceMap[currentPathElement].(map[string]interface{})

	if !ok {
		return nil, fmt.Errorf("error upon attempt to parse value at '%s' into map for further processing", currentPathElement)
	}

	keyPath = keyPath[strings.Index(keyPath, ".")+1:]

	return ExtractConfigValue(sourceMap, keyPath)

}
