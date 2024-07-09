// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package out

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

// ParseFileArray parses the given file and returns the array of values within
// it. The file can be keyed yaml, keyed json, or raw-value tab/space delimited
// lines in a text file. Files with no extension are assumed to be text files.
// The input type must be a struct with public fields. As an example,
//
//	ParseFileArray[struct{
//		F1 string `json:"f1" yaml:"f1"`
//		F2 int    `json:"f2" yaml:"f2"`
//	}]
//
// For text files, if the line is empty or begins with "// ", the line is
// skipped.
func ParseFileArray[T any](fs afero.Fs, file string) ([]T, error) {
	var unmarshal func([]byte, interface{}) error
	switch ext := filepath.Ext(file); ext {
	case ".txt", "":
		f, err := fs.Open(file)
		if err != nil {
			return nil, fmt.Errorf("unable to open %q: %v", file, err)
		}
		defer f.Close()
		vs, err := parseSpaceLines[T](f)
		if err != nil {
			return nil, fmt.Errorf("unable to process file %q: %v", file, err)
		}
		return vs, nil
	case ".yml", ".yaml":
		unmarshal = yaml.Unmarshal
	case ".json":
		unmarshal = json.Unmarshal
	default:
		return nil, fmt.Errorf("unable to handle file %q extension %s", file, ext)
	}

	raw, err := afero.ReadFile(fs, file)
	if err != nil {
		return nil, fmt.Errorf("unable to read file %q: %v", file, err)
	}
	var vs []T
	if err := unmarshal(raw, &vs); err != nil {
		return nil, fmt.Errorf("unable to process file %q: %v", file, err)
	}
	return vs, nil
}

func parseSpaceLines[T any](r io.Reader) ([]T, error) {
	var typ reflect.Type
	{
		var v T
		typ = reflect.TypeOf(v)
	}
	if typ.Kind() != reflect.Struct {
		return nil, errors.New("internal type to decode into is not a struct")
	}

	var vs []T
	s := bufio.NewScanner(r)
	for s.Scan() {
		line := s.Text()
		if len(line) == 0 || strings.HasPrefix(line, "// ") {
			continue
		}
		fields := strings.Split(line, " ")
		if len(fields) != typ.NumField() {
			fields = strings.Split(line, "\t")
			if len(fields) != typ.NumField() {
				return nil, fmt.Errorf("short line: saw %d out of %d fields", len(fields), typ.NumField())
			}
		}

		var v T
		val := reflect.Indirect(reflect.ValueOf(&v))
		for i := 0; i < typ.NumField(); i++ {
			sf := val.Field(i)
			f := fields[i]
			switch {
			case sf.Type().Kind() == reflect.String:
				sf.SetString(f)
			case sf.Type().Kind() == reflect.Bool:
				p, err := strconv.ParseBool(f)
				if err != nil {
					return nil, fmt.Errorf("unable to decode %s as a bool", f)
				}
				sf.SetBool(p)
			case sf.CanInt():
				p, err := strconv.ParseInt(f, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("unable to decode %s as an int", f)
				}
				sf.SetInt(p)
			case sf.CanUint():
				p, err := strconv.ParseUint(f, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("unable to decode %s as a uint", f)
				}
				sf.SetUint(p)
			case sf.CanFloat():
				p, err := strconv.ParseFloat(f, 64)
				if err != nil {
					return nil, fmt.Errorf("unable to decode %s as a float", f)
				}
				sf.SetFloat(p)
			default:
				return nil, fmt.Errorf("internal type to decode into has unhandled field type %v", sf.Type().Kind())
			}
		}
		vs = append(vs, v)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return vs, nil
}

// ParseTopicPartitions parses a topic:pa,rt,it,io,ns flag.
func ParseTopicPartitions(list []string) (map[string][]int32, error) {
	tps := make(map[string][]int32)
	for _, item := range list {
		split := strings.SplitN(item, ":", 2)
		if len(split) == 1 {
			tps[split[0]] = nil
			continue
		}

		strParts := strings.Split(split[1], ",")
		i32Parts := make([]int32, 0, len(strParts))

		for _, strPart := range strParts {
			part, err := strconv.ParseInt(strPart, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("item %q part %q parse err %w", item, strPart, err)
			}
			i32Parts = append(i32Parts, int32(part))
		}
		tps[split[0]] = i32Parts
	}
	return tps, nil
}

var (
	partitionRe     *regexp.Regexp
	partitionReOnce sync.Once
)

// ParsePartitionString parses a partition string with the format:
// {namespace}/{topic}/[partitions...]
// where namespace and topic are optionals, and partitions are comma-separated
// partitions ID. If namespace is not provided, the function assumes 'kafka'.
func ParsePartitionString(ntp string) (ns, topic string, partitions []int, rerr error) {
	partitionReOnce.Do(func() {
		// Matches {namespace}/{topic}/[partitions...]
		// - Index 0: Full Match.
		// - Index 1: Namespace, if present.
		// - Index 2: Topic, if present.
		// - Index 3: Comma-separated partitions.
		partitionRe = regexp.MustCompile(`^(?:(?:([^/]+)/)?([^/]+)/)?(\d+(?:,\d+)*)$`)
	})
	match := partitionRe.FindStringSubmatch(ntp)
	if len(match) == 0 {
		return "", "", nil, fmt.Errorf("unable to parse %q: wrong format", ntp)
	}
	ns = match[1]
	if ns == "" {
		ns = "kafka"
	}
	partitionString := strings.Split(match[3], ",")
	for _, str := range partitionString {
		p, err := strconv.Atoi(str)
		if err != nil {
			return "", "", nil, fmt.Errorf("unable to parse partition %v from string %v: %v", str, ntp, err)
		}
		partitions = append(partitions, p)
	}
	return ns, match[2], partitions, nil
}

// ParseFileOrStringFlag parses a flag string, if it starts with '@' it
// will treat it as a filepath and will attempt to read the file.
func ParseFileOrStringFlag(fs afero.Fs, flag string) ([]byte, error) {
	if strings.HasPrefix(flag, "@") {
		file, err := afero.ReadFile(fs, strings.TrimPrefix(flag, "@"))
		if err != nil {
			return nil, fmt.Errorf("unable to read file: %v", err)
		}
		return file, nil
	}
	return []byte(flag), nil
}
