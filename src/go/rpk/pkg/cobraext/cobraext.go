// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cobraext

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Walk calls f for c and all of its children.
func Walk(c *cobra.Command, f func(*cobra.Command)) {
	f(c)
	for _, c := range c.Commands() {
		Walk(c, f)
	}
}

// DeprecatedCmd returns a new no-op command with the given name.
func DeprecatedCmd(name string, args int) *cobra.Command {
	return &cobra.Command{
		Use:   name,
		Short: "This command has been deprecated.",
		Args:  cobra.ExactArgs(args),
		Run:   func(_ *cobra.Command, _ []string) {},
	}
}

// DeprecateCmd marks a command as deprecated and, when a person uses the
// command, writes the new command a user should use. If newUse is empty, the
// output is that this command does nothing and is a no-op.
func DeprecateCmd(newCmd *cobra.Command, newUse string) *cobra.Command {
	newCmd.Deprecated = fmt.Sprintf("use %q instead", newUse)
	if newUse == "" {
		newCmd.Deprecated = "this command is now a no-op"
	}
	newCmd.Hidden = true
	if children := newCmd.Commands(); len(children) > 0 {
		for _, child := range children {
			DeprecateCmd(child, newUse+" "+child.Name())
		}
	}
	return newCmd
}

// StripFlagset removes all flags and potential values from args that are in
// the flagset.
func StripFlagset(args []string, fs *pflag.FlagSet) (keep, stripped []string) {
	var long, short []string
	fs.VisitAll(func(f *pflag.Flag) {
		long = append(long, f.Name)
		short = append(short, f.Shorthand)
	})
	return StripFlags(args, fs, long, short)
}

// StripFlags removes all long and short flags and their values from the input
// args set. The flags must be defined in the flag set.
func StripFlags(args []string, fs *pflag.FlagSet, long []string, short []string) (keep, stripped []string) {
	stripLong := make(map[string]struct{}, len(long))
	for _, f := range long {
		if len(f) > 0 {
			stripLong[f] = struct{}{}
		}
	}
	stripShort := make(map[string]struct{}, len(short))
	for _, f := range short {
		if len(f) > 0 {
			stripShort[f] = struct{}{}
		}
	}

	keep = args[:0]

	var inFlag bool
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			keep = append(keep, args[i:]...) // flag parsing stops at --
			break
		}

		if inFlag {
			inFlag = false
			keep = append(keep, arg) // we just kept a flag and are now keeping its value
			continue
		}

		kv := strings.SplitN(arg, "=", 2)
		k := kv[0]

		switch {
		case strings.HasPrefix(k, "--"):
			k = k[2:]
			f := fs.Lookup(k)
			_, strip := stripLong[k]
			if f == nil || !strip {
				inFlag = len(kv) == 1
				// either we are keeping this flag, or it is not in our flag
				// set, so we cannot strip it.
				keep = append(keep, arg)
				// If we are at this point is because the flag does not exist,
				// and it's part of the plugin. If the next arg starts with
				// '-' we will assume this flag is a boolean flag.
				if i+1 < len(args) && strings.HasPrefix(args[i+1], "-") {
					inFlag = false
				}
				continue
			}

			// We are stripping this flag. If we require a value
			// (!NoOptDefVal) and we do not have one (from k=v),
			// then we skip the next argument because it must be
			// the flag value.
			needV := f.NoOptDefVal == ""
			stripped = append(stripped, arg)
			if needV && len(kv) == 1 {
				i++
				if i < len(args) {
					stripped = append(stripped, args[i])
				}
			}

		case strings.HasPrefix(k, "-"):
			var (
				ks       = k[1:]
				keepArg  = "-"
				stripArg = "-"
				stripVal *string
			)
			// Short string
			// can be defined in one long run of letters.
			for ik := 0; ik < len(ks); ik++ {
				k := string(ks[ik])
				f := fs.ShorthandLookup(k)
				_, strip := stripShort[k]
				if f == nil || !strip {
					keepArg += k
					if ik == len(ks)-1 {
						if len(kv) > 1 {
							keepArg += "=" + kv[1]
						} else {
							inFlag = true
						}
					}
					continue
				}

				// We are stripping this short flag. If the
				// short flag requires a value, we should skip
				// the value (next argument) no matter what.
				// However, if the user is doing something
				// wrong and this short-flag-requiring-value is
				// in the middle of other short flags, we do
				// not skip a value.
				needV := f.NoOptDefVal == ""
				stripArg += k
				if needV && len(kv) == 1 && ik == len(ks)-1 {
					i++
					if i < len(args) {
						stripVal = &args[i]
					}
				}
				if ik == len(ks)-1 && len(kv) == 2 {
					stripArg += "=" + kv[1]
				}
			}
			// Only keep this partial-arg if we kept any short
			// flags.
			if keepArg != "-" {
				keep = append(keep, keepArg)
			}
			if stripArg != "-" {
				stripped = append(stripped, stripArg)
				if stripVal != nil {
					stripped = append(stripped, *stripVal)
				}
			}

		default:
			keep = append(keep, arg) // this must be a command or argument
		}
	}

	return keep, stripped
}

// LongFlagValue returns the value for the given long flag (without the dash
// prefix) if it exists. This takes the full flagset as a hint for how to parse
// some flags in args. The flagset should be received from *inside* a cobra
// command, where persistent and non-persistent flags from all parents are
// merged. For repeated flags, only the last value is returned.
func LongFlagValue(args []string, fs *pflag.FlagSet, flag, shorthand string) string {
	nop := new(nopValue)
	dup := pflag.NewFlagSet("dup", pflag.ContinueOnError)
	dup.ParseErrorsWhitelist = pflag.ParseErrorsWhitelist{UnknownFlags: true}

	var f string
	dup.StringVarP(&f, flag, shorthand, "", "")
	added := dup.Lookup(flag)
	fs.VisitAll(func(f *pflag.Flag) {
		if f.Name != flag {
			f2 := *f
			f2.Value = nop
			dup.AddFlag(&f2)
		} else {
			v := added.Value
			*added = *f
			added.Value = v
		}
	})
	dup.Parse(args)
	return f
}

type nopValue struct{}

func (*nopValue) String() string   { return "" }
func (*nopValue) Set(string) error { return nil }
func (*nopValue) Type() string     { return "" }
