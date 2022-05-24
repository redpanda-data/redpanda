// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"
)

// This file contains weak params type, including basic types support (bool,
// int, and string) and one_or_many support for different types.
//
// The use of this file is to support our transition to a strongly typed
// config file and our migration away from cobra and mapstructure.

// weakBool is an intermediary boolean type to be used during our transition
// to strictly typed configuration parameters. This will allow us to support
// weakly typed parsing:
//
//   - int to bool (true if value != 0)
//   - string to bool (accepts: 1, t, T, TRUE, true, True, 0, f, F, FALSE,
//     false, False. Anything else is an error)
type weakBool bool

func (wb *weakBool) UnmarshalYAML(n *yaml.Node) error {
	switch n.Tag {
	case "!!bool":
		b, err := strconv.ParseBool(n.Value)
		if err != nil {
			return err
		}
		*wb = weakBool(b)
		return nil
	case "!!int":
		ni, err := strconv.Atoi(n.Value)
		if err != nil {
			return fmt.Errorf("cannot parse '%s' as bool: %s", n.Value, err)
		}
		fmt.Println("deprecation warning: type conversion from integers to boolean will be deprecated in v23.2.1")
		*wb = ni != 0
		return nil
	case "!!str":
		// it accepts 1, t, T, TRUE, true, True, 0, f, F
		nb, err := strconv.ParseBool(n.Value)
		if err == nil {
			fmt.Println("deprecation warning: type conversion from string to boolean will be deprecated in v23.2.1")
			*wb = weakBool(nb)
			return nil
		} else if n.Value == "" {
			fmt.Println("deprecation warning: type conversion from string to boolean will be deprecated in v23.2.1")
			*wb = false
			return nil
		} else {
			return fmt.Errorf("cannot parse '%s' as bool: %s", n.Value, err)
		}
	default:
		return fmt.Errorf("type %s not supported as a boolean", n.Tag)
	}
}

// weakInt is an intermediary integer type to be used during our transition to
// strictly typed configuration parameters. This will allow us to support
// weakly typed parsing:
//
//   - strings to int/uint (base implied by prefix)
//   - bools to int/uint (true = 1, false = 0)
type weakInt int

func (wi *weakInt) UnmarshalYAML(n *yaml.Node) error {
	switch n.Tag {
	case "!!int":
		ni, err := strconv.Atoi(n.Value)
		if err != nil {
			return err
		}
		*wi = weakInt(ni)
		return nil
	case "!!str":
		str := n.Value
		if str == "" {
			str = "0"
		}
		ni, err := strconv.Atoi(str)
		if err != nil {
			return fmt.Errorf("cannot parse '%s' as an integer: %s", str, err)
		}
		fmt.Println("deprecation warning: type conversion from string to integer will be deprecated in v23.2.1")
		*wi = weakInt(ni)
		return nil
	case "!!bool":
		nb, err := strconv.ParseBool(n.Value)
		if err != nil {
			return fmt.Errorf("cannot parse '%s' as an integer: %s", n.Value, err)
		}
		fmt.Println("deprecation warning: type conversion from boolean to integer will be deprecated in v23.2.1")
		if nb {
			*wi = 1
			return nil
		}
		*wi = 0
		return nil
	default:
		return fmt.Errorf("type %s not supported as an integer", n.Tag)
	}
}

// weakString is an intermediary string type to be used during our transition to
// strictly typed configuration parameters. This will allow us to support
// weakly typed parsing:
//
//   - bools to string (true = "1", false = "0")
//   - numbers to string (base 10)
type weakString string

func (ws *weakString) UnmarshalYAML(n *yaml.Node) error {
	switch n.Tag {
	case "!!str":
		*ws = weakString(n.Value)
		return nil
	case "!!bool":
		nb, err := strconv.ParseBool(n.Value)
		if err != nil {
			return fmt.Errorf("cannot parse '%s' as a boolean: %s", n.Value, err)
		}
		fmt.Println("deprecation warning: type conversion from boolean to string will be deprecated in v23.2.1")
		if nb {
			*ws = "1"
			return nil
		}
		*ws = "0"
		return nil
	case "!!int", "!!float":
		fmt.Println("deprecation warning: type conversion from numbers to string will be deprecated in v23.2.1")
		*ws = weakString(n.Value)
		return nil
	default:
		return fmt.Errorf("type %s not supported as a string", n.Tag)
	}
}

// NamedSocketAddresses is an intermediary one_or_many type to be used
// during our transition to strictly typed configuration parameters.
// This type will:
//   - parse an array of namedSocketAddress
//   - parse a single namedSocketAddress to an array.
type NamedSocketAddresses []NamedSocketAddress

func (a *NamedSocketAddresses) UnmarshalYAML(n *yaml.Node) error {
	var multi []NamedSocketAddress
	err := n.Decode(&multi)
	if err == nil {
		*a = multi
		return nil
	}

	var single NamedSocketAddress
	err = n.Decode(&single)
	if err != nil {
		return err
	}
	fmt.Println("deprecation warning: single element named socket addresses will be deprecated in v23.2.1")
	*a = []NamedSocketAddress{single}
	return nil
}

// ServerTLSArray is an intermediary one_or_many type to be used during our
// transition to strictly typed configuration parameters. This type will:
//   - parse an array of ServerTLS
//   - parse a single ServerTLS to an array.
type ServerTLSArray []ServerTLS

func (a *ServerTLSArray) UnmarshalYAML(n *yaml.Node) error {
	var multi []ServerTLS
	err := n.Decode(&multi)
	if err == nil {
		*a = multi
		return nil
	}

	var single ServerTLS
	err = n.Decode(&single)
	if err != nil {
		return err
	}
	fmt.Println("deprecation warning: single element server TLS will be deprecated in v23.2.1")
	*a = []ServerTLS{single}
	return nil
}
