// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package generate

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

var (
	//go:embed app-templates/*
	tplFS      embed.FS
	fileTplMap = map[string]*fileTmplDesc{
		"go": {
			fileSpec: &fileSpec{
				Location:    "go-produce-consume.tar.gz",
				Description: "Go sample application - creates a topic, produce and consume to it.",
				Hash:        "86c7230e2267caafdd0306b14842a0807da30c262fcf84eb62e55b5abdf5fe1d",
			},
			instruction: `To create the demo topic: 
  go run admin/admin.go

To produce to the topic: 
  go run producer/producer.go

To consume the data back:
  go run consumer/consumer.go
`,
		},
	}
	languages = []string{"Go"}
)

// templateDir is the templates dir name.
const templateDir = "app-templates"

type fileTmplDesc struct {
	*fileSpec
	instruction string
}

type tmplData struct {
	Password   string
	ScramBits  string
	SeedServer []string
	Username   string
	IsTLS      bool
}

func newAppCmd(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		langFlag string
		outPath  string
		saslCred string
		noUser   bool
	)
	cmd := &cobra.Command{
		Use:   "app",
		Short: "Generate a sample application to connect with Redpanda",
		Long:  appHelpText,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			isInteractive := langFlag == "" // The rest of the flags are optional.
			if !isInteractive {
				if saslCred != "" {
					out.Die("--new-sasl-credentials flag must be used with --language flag")
				} else if noUser {
					out.Die("--no-user flag must be used with --language flag")
				}
			}

			var lang, user, pass string
			if isInteractive {
				lang, user, pass, err = runInteractive(cmd.Context(), fs, p)
				out.MaybeDieErr(err)
			} else {
				lang, user, pass, err = runWithFlags(cmd.Context(), fs, p, langFlag, saslCred, noUser)
				out.MaybeDieErr(err)
			}
			data := tmplData{
				SeedServer: p.KafkaAPI.Brokers,
				Username:   user,
				Password:   pass,
				IsTLS:      p.KafkaAPI.TLS != nil,
				ScramBits:  scramBits(p.KafkaAPI.SASL),
			}

			f, ok := fileTplMap[lang]
			// Unlikely since we check that the user only selects the allowed
			// languages, but better be safe.
			if !ok {
				out.Die("unable to create a sample app for the selected language: %q", lang)
			}
			dir, err := extractTarGzAndReplace(tplFS, fs, data, filepath.Join(templateDir, f.Location), outPath)
			out.MaybeDie(err, "unable to extract tarball: %v", err)

			if user != "" {
				fmt.Printf("Successfully created the sample app with user %q in:\n\n", user)
			} else {
				fmt.Printf("Successfully created the sample app in:\n\n")
			}
			err = printInstructions(fs, dir, lang)
			if err != nil {
				fmt.Fprintf(os.Stderr, "unable to print the app instructions: %v; you may find the instructions in the README file inside the generated app\n", err)
			}
		},
	}

	cmd.Flags().StringVarP(&langFlag, "language", "l", "", "The language you want the code sample to be generated with")
	cmd.Flags().StringVarP(&outPath, "output", "o", "", "The path where the app will be written")
	cmd.Flags().StringVar(&saslCred, "new-sasl-credentials", "", "If provided, rpk will generate and use these credentials (<user>:<password>)")
	cmd.Flags().BoolVar(&noUser, "no-user", false, "Generates the sample app without SASL user")

	return cmd
}

func runInteractive(ctx context.Context, fs afero.Fs, p *config.RpkProfile) (lang, user, pass string, err error) {
	// Kind of user options in the interactive mode.
	const (
		optCreate = iota
		optExisting
		optSkip
	)

	lang, err = out.Pick(languages, "Choose a language for your sample app:")
	if err != nil {
		return "", "", "", fmt.Errorf("error picking the language: %v", err)
	}
	lang = strings.ToLower(lang)

	optMap := map[string]int{
		"Create new user with admin ACLs": optCreate,
		"Skip":                            optSkip,
	}
	if p.HasSASLCredentials() {
		msg := fmt.Sprintf("Use existing profile user [%s]", p.KafkaAPI.SASL.User)
		optMap[msg] = optExisting
	}

	userOpts := make([]string, 0, len(optMap))
	for k := range optMap {
		userOpts = append(userOpts, k)
	}
	sort.Slice(userOpts, func(i, j int) bool {
		return optMap[userOpts[i]] < optMap[userOpts[j]] // Sort based on the enum.
	})

	userOptPick, err := out.Pick(userOpts, "Choose a user:")
	if err != nil {
		return "", "", "", fmt.Errorf("error picking the user options: %v", err)
	}

	switch optMap[userOptPick] {
	case optCreate:
		user, err = out.Prompt("Enter a username:")
		if err != nil {
			return "", "", "", fmt.Errorf("error picking the user: %v", err)
		}
		pass, err = out.PromptPassword("Enter a password:")
		if err != nil {
			return "", "", "", fmt.Errorf("error picking the user: %v", err)
		}
		err = createUser(ctx, fs, p, user, pass)
		if err != nil {
			return "", "", "", err
		}
		return lang, user, pass, nil
	case optExisting:
		return lang, p.KafkaAPI.SASL.User, p.KafkaAPI.SASL.Password, nil
	case optSkip:
		return lang, "", "", nil
	}
	return // unreachable
}

func runWithFlags(ctx context.Context, fs afero.Fs, p *config.RpkProfile, langFlag, saslCred string, noUser bool) (lang, user, pass string, err error) {
	switch strings.ToLower(langFlag) {
	case "go", "golang":
		lang = "go"
	default:
		return "", "", "", fmt.Errorf("unsupported --language %q; currently supported languages: %v", langFlag, languages)
	}
	if noUser { // only parse language, leave user and pass empty.
		return
	}
	if saslCred != "" {
		s := strings.SplitN(saslCred, ":", 2)
		if len(s) < 2 {
			return "", "", "", fmt.Errorf("unable to parse %q: credentials does not follow the <user>:<pass> format", saslCred)
		}
		user = s[0]
		pass = s[1]
		err = createUser(ctx, fs, p, user, pass)
		if err != nil {
			return "", "", "", err
		}
	} else {
		if p.HasSASLCredentials() {
			user = p.KafkaAPI.SASL.User
			pass = p.KafkaAPI.SASL.Password
		}
	}
	return
}

func createUser(ctx context.Context, fs afero.Fs, p *config.RpkProfile, user, pass string) error {
	cl, err := adminapi.NewClient(ctx, fs, p)
	if err != nil {
		return fmt.Errorf("unable to initialize admin client: %v", err)
	}
	err = cl.CreateUser(ctx, user, pass, adminapi.ScramSha256)
	if err != nil {
		return fmt.Errorf("unable to create user %q: %v", user, err)
	}
	adm, err := kafka.NewAdmin(fs, p)
	if err != nil {
		return fmt.Errorf("unable to initialize kafka client: %v", err)
	}
	// Read-Write ACL:
	b := kadm.NewACLs().
		ResourcePatternType(kadm.ACLPatternLiteral).
		Allow(user).
		Clusters().
		Topics("*").
		Groups("*").
		TransactionalIDs("*").
		Operations(kadm.OpRead, kadm.OpWrite, kadm.OpCreate, kadm.OpDescribe, kadm.OpDelete)
	b.PrefixUser()
	_, err = adm.CreateACLs(ctx, b)
	if err != nil {
		return fmt.Errorf("unable to Create ACL for user %v: %v", user, err)
	}
	fmt.Printf("Successfully created user %q\n", user)
	return nil
}

// extractTarGzAndReplace extracts and expand the sample app directory to destFS
// and replaces the values with the passed tmplData. Returns the path where file
// was extracted.
func extractTarGzAndReplace(tplFS iofs.FS, destFS afero.Fs, data tmplData, file, out string) (string, error) {
	r, err := tplFS.Open(file)
	if err != nil {
		return "", err
	}
	gr, err := gzip.NewReader(r)
	if err != nil {
		return "", err
	}
	tr := tar.NewReader(gr)
	var extractPath string
	for {
		fh, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		switch fh.Typeflag {
		case tar.TypeDir:
			// This is saved the first time only.
			if extractPath == "" {
				dir := fh.Name
				if out != "" {
					dir = filepath.Join(out, fh.Name)
				}
				extractPath = dir
			}
		case tar.TypeReg:
			// The file is a Go template, we parse it and copy it to the
			// destination with the values replaced.
			b, err := io.ReadAll(tr)
			if err != nil {
				return "", err
			}
			tpl, err := template.New("sample").Parse(string(b))
			if err != nil {
				return "", err
			}
			dest := fh.Name
			if out != "" {
				dest = filepath.Join(out, fh.Name)
			}
			var buf bytes.Buffer
			err = tpl.Execute(&buf, data)
			if err != nil {
				return "", err
			}
			err = rpkos.ReplaceFile(destFS, dest, buf.Bytes(), 0o644)
			if err != nil {
				return "", err
			}
		}
	}

	return filepath.Clean(extractPath), nil
}

func scramBits(sasl *config.SASL) string {
	if sasl == nil {
		return "256"
	}
	switch sasl.Mechanism {
	case adminapi.ScramSha512:
		return "512"
	default:
		return "256"
	}
}

// printInstructions prints the directory tree and a quick guide on how to
// run the app.
func printInstructions(fs afero.Fs, dir, lang string) error {
	fmt.Println(dir)
	err := rpkos.PrintDirectoryTree(fs, dir, "")
	if err != nil {
		return fmt.Errorf("unable to print directory tree: %v", err)
	}
	fmt.Printf("\nTo run the application:\n  cd %v\n\n", dir)
	fmt.Println(fileTplMap[lang].instruction)
	return nil
}

const appHelpText = `Generate a sample application to connect with Redpanda.

This command generates a starter application to produce and consume from the
settings defined in the rpk profile. Its goal is to get you producing and
consuming quickly with Redpanda in a language that is familiar to you.

By default, this will run interactively, prompting you to select a language and
a user with which to create your application. To use this without interactivity,
specify how you would like your application to be created using flags.

The --language option allows you to specify the language. There is no default.

The --new-sasl-credentials <user>:<password> allows you to generate a new SASL
user with admin ACLs. If you don't want to use your current profile user nor
create a new one, you may use --no-user flag to generate the starter app without
the user.

If you are having trouble connecting to your cluster, you can use -X
admin.hosts=comma,delimited,host:ports to pass a specific admin api address.

EXAMPLES

Generate an app with interactive prompts:
  rpk generate app

Generate an app in a specified language with the existing SASL user:
  rpk generate app --language <lang>

Generate an app in the specified language with a new SASL user:
  rpk generate app -l <lang> --new-sasl-credentials <user>:<password>

Generate an app in the 'tmp' dir, but take no action on the user:
  rpk generate app -l <lang> --no-user --output /tmp
`
