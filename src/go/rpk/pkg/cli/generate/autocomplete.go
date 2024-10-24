// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package generate

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func newShellCompletionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "shell-completion",
		Short: "Generate shell completion commands",
		Long: `
Shell completion can help autocomplete rpk commands when you press tab.

BASH

Bash autocompletion relies on the bash-completion package. You can test if you
have this by running "type _init_completion", if you do not, you can install
the package through your package manager.

If you have bash-completion installed, and the command still fails, you likely
need to add the following line to your ~/.bashrc:

    source /usr/share/bash-completion/bash_completion

To ensure autocompletion of rpk exists in all shell sessions, add the following
to your ~/.bashrc:

    command -v rpk >/dev/null && . <(rpk generate shell-completion bash)

Alternatively, to globally enable rpk completion, you can run the following:

    rpk generate shell-completion bash > /etc/bash_completion.d/rpk

ZSH

If shell completion is not already enabled in your environment, you will need to
enable it. You can execute the following once:

  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

To enable autocompletion in any zsh session for any user, run this once:

# Linux:

    rpk generate shell-completion zsh > "${fpath[1]}/_rpk"

# macOS:

     rpk generate shell-completion zsh > "$(brew --prefix)/share/zsh/site-functions/_rpk"

You can also place that command in your ~/.zshrc to ensure that when you update
rpk, you update autocompletion. If you initially require sudo to edit that
file, you can chmod it to be world writeable, after which you will always be
able to update it from ~/.zshrc.

FISH

To enable autocompletion in any fish session, run:

    rpk generate shell-completion fish > ~/.config/fish/completions/rpk.fish
`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 || len(args) > 1 {
				cmd.Help()
				return
			}
			switch shell := args[0]; shell {
			case "bash":
				cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				cmd.Root().GenZshCompletion(os.Stdout)
				// https://github.com/spf13/cobra/issues/1529
				fmt.Fprint(os.Stdout, "\ncompdef _rpk rpk\n")
			case "fish":
				cmd.Root().GenFishCompletion(os.Stdout, true)
			default:
				zap.L().Sugar().Fatalf("unrecognized shell %s", shell)
			}
		},
	}
}
