// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newPromptCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var validate bool
	cmd := &cobra.Command{
		Use:   "prompt",
		Short: "Prompt a profile name formatted for a PS1 prompt",
		Long: `Prompt a profile name formatted for a PS1 prompt.

This command prints ANSI-escaped text per your current profile's "prompt"
field. If the current profile does not have a prompt, this prints nothing.
If the prompt is invalid, this exits 0 with no message. To validate the
current prompt, use the --validate flag.

This command may introduce other % variables in the future, if you want to
print a % directly, use %% to escape it.

To use this in zsh, be sure to add setopt PROMPT_SUBST to your .zshrc.
To edit your PS1, use something like PS1='$(rpk profile prompt)' in your
shell rc file.

FORMAT

The "prompt" field supports space or comma separated modifiers and a quoted
string that is be modified. Inside the string, the variable %p or %n refers to
the profile name. As a few examples:

    prompt: hi-white, bg-red, bold, "[%p]"
    prompt: hi-red  "PROD"
    prompt: white, "dev-%n

If you want to have multiple formats, you can wrap each formatted section in
parentheses.

    prompt: ("--") (hi-white bg-red bold "[%p]")

COLORS

All ANSI colors are supported, with names matching the color name:
"black", "red", "green", "yellow", "blue", "magenta", "cyan", "white".

The "hi-" prefix indicates a high-intensity color: "hi-black", "hi-red", etc.
The "bg-" prefix modifies the background color: "bg-black", "bg-hi-red", etc.

MODIFIERS

Four modifiers are supported, "bold", "faint", "underline", and "invert".
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			var errmsg string
			if validate {
				defer func() {
					if errmsg != "" {
						out.DieString(errmsg)
					}
				}()
			}

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				errmsg = "rpk.yaml file is missing or cannot be loaded"
				return
			}
			p := y.Profile(y.CurrentProfile)
			if p == nil {
				errmsg = fmt.Sprintf("current profile %q does not exist", y.CurrentProfile)
				return
			}

			prompt := cfg.VirtualRpkYaml().Globals.Prompt
			if p.Prompt != "" {
				prompt = p.Prompt
			}

			parens, err := splitPromptParens(prompt)
			if err != nil {
				errmsg = err.Error()
				return
			}

			var sb strings.Builder
			for _, g := range parens {
				text, attrs, err := parsePrompt(g, p.Name)
				if err != nil {
					errmsg = err.Error()
					return
				}
				c := color.New(attrs...)
				c.EnableColor()
				sb.WriteString(c.Sprint(text))
			}
			if validate {
				fmt.Print("Prompt ok! Output:\nvvv\n")
				defer fmt.Print("\n^^^\n")
			}
			fmt.Print(sb.String())
		},
	}
	cmd.Flags().BoolVar(&validate, "validate", false, "Exit with an error message if the prompt is invalid")
	return cmd
}

func parsePrompt(s string, name string) (string, []color.Attribute, error) {
	var (
		b       = make([]byte, 0, 16) // current text or attribute buffer
		text    []byte                // if non nil, this has been initialized; we cannot have multiple quoted strings
		attrs   []color.Attribute
		inQuote bool
	)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\':
			if !inQuote {
				return "", nil, errors.New("backslash is only allowed inside a quoted string")
			}
			if len(s) > i+1 {
				b = append(b, s[i+1])
				i++
			}
		case '"':
			if inQuote {
				text = append(text, b...)
				b = b[:0]
				inQuote = false
			} else if text != nil {
				return "", nil, errors.New("only one quoted string can appear in a prompt, we saw a second")
			} else {
				b = bytes.TrimSpace(b)
				if len(b) > 0 {
					return "", nil, fmt.Errorf("unexpected text %q before quoted string", string(b))
				}
				inQuote = true
			}
		case ' ', '\t', ',':
			if inQuote {
				b = append(b, c)
				continue
			}
			b = bytes.TrimSpace(b)
			if len(b) == 0 {
				continue
			}
			attr, ok := out.ParseColor(string(b))
			if !ok {
				return "", nil, fmt.Errorf("invalid color or attribute %q", string(b))
			}
			attrs = append(attrs, attr)
			b = b[:0]
		default:
			b = append(b, c)
		}
	}

	// If b is non-empty, the prompt either ended in unneeded spaces or it
	// ended in an attr -- any ending quote is handled in the above block.
	if b = bytes.TrimSpace(b); len(b) > 0 {
		attr, ok := out.ParseColor(string(b))
		if !ok {
			return "", nil, fmt.Errorf("invalid color or attribute %q", string(b))
		}
		attrs = append(attrs, attr)
	}

	output := make([]byte, 0, len(s)+len(text))
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch c {
		case '%':
			if len(text) > i+1 {
				switch text[i+1] {
				case 'p', 'n':
					output = append(output, name...)
					i++
				case '%':
					output = append(output, '%')
					i++
				default:
					return "", nil, fmt.Errorf("unknown escape %%%c", text[i+1])
				}
			}
		default:
			output = append(output, c)
		}
	}
	if len(s) != 0 && len(text) == 0 {
		output = append(output, name...)
	}
	return string(output), attrs, nil
}

func splitPromptParens(s string) ([]string, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return nil, nil
	}

	var (
		parens    []string
		current   []byte
		onlyParen = s[0] != '('
		inParen   = onlyParen
		inQuote   bool
		inEsc     bool
	)

	for i := 0; i < len(s); i++ {
		c := s[i]

		if !inParen {
			switch c {
			case ' ', '\t', ',':
				continue
			case '(':
				inParen = true
				continue
			default:
				return nil, fmt.Errorf("unexpected character %c while looking for paren group", c)
			}
		}

		if inEsc {
			inEsc = false
			current = append(current, c)
			continue
		}

		if inQuote {
			if c == '\\' {
				inEsc = true
			} else if c == '"' {
				inQuote = false
			}
			current = append(current, c)
			continue
		}

		switch c {
		case '\\':
			inEsc = true
		case '"':
			inQuote = true
		case ')':
			if onlyParen {
				return nil, errors.New("unexpected closing paren )")
			}
			inParen = false
			current = bytes.TrimSpace(current)
			if len(current) > 0 {
				parens = append(parens, string(current))
			}
			current = current[:0]
			continue
		}
		current = append(current, c)
	}
	if onlyParen {
		if len(current) > 0 {
			parens = append(parens, string(current))
		}
	} else if len(current) > 0 {
		return nil, errors.New("prompt is missing closing paren")
	}
	return parens, nil
}
