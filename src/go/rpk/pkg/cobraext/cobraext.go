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

// StripFlagset removes all flags and potential values from args that are in
// the flagset.
func StripFlagset(args []string, fs *pflag.FlagSet) []string {
	var long, short []string
	fs.VisitAll(func(f *pflag.Flag) {
		long = append(long, f.Name)
		short = append(short, f.Shorthand)
	})
	return StripFlags(args, fs, long, short)
}

// StripFlags removes all long and short flags and their values from the input
// args set. The flags must be defined in the flag set.
func StripFlags(args []string, fs *pflag.FlagSet, long []string, short []string) []string {
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

	keep := args[:0]

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
				keep = append(keep, arg) // either we are keeping this flag, or it is not in our flag set so we cannot strip it
				continue
			}

			// We are stripping this flag. If we require a value
			// (!NoOptDefVal) and we do not have one (from k=v),
			// then we skip the next argument because it must be
			// the flag value.
			needV := f.NoOptDefVal == ""
			if needV && len(kv) == 1 {
				i++
			}

		case strings.HasPrefix(k, "-"):
			ks := k[1:]
			keepArg := "-"
			// Short flags are more complicated because short flags
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
				if needV && len(kv) == 1 && ik == len(ks)-1 {
					i++
				}
			}
			// Only keep this partial-arg if we kept any short
			// flags.
			if keepArg != "-" {
				keep = append(keep, keepArg)
			}

		default:
			keep = append(keep, arg) // this must be a command or argument
		}
	}

	return keep
}
