// Copyright 2026 Redpanda Data, Inc.
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
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const (
	skillsRepo      = "redpanda-data/skills"
	skillsRepoURL   = "https://github.com/" + skillsRepo
	defaultProvider = "claude"
)

// skillProvider describes where an AI coding assistant loads skills from and
// how to refer to it in output.
type skillProvider struct {
	name        string   // canonical flag value
	displayName string   // human-friendly
	dir         []string // install path segments under the user home dir
}

var skillProviders = map[string]skillProvider{
	"claude": {
		name:        "claude",
		displayName: "Claude Code",
		dir:         []string{".claude", "skills"},
	},
}

// defaultDir resolves the provider's skills directory under the user home.
func (p skillProvider) defaultDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("unable to determine your home directory: %v", err)
	}
	return filepath.Join(append([]string{home}, p.dir...)...), nil
}

func sortedProviderNames() []string {
	p := slices.Collect(maps.Keys(skillProviders))
	slices.Sort(p)
	return p
}

func newSkillCmd(fs afero.Fs) *cobra.Command {
	var (
		provider string
		branch   string
		dir      string
		skills   string
		all      bool
	)
	cmd := &cobra.Command{
		Use:   "skill",
		Short: "Install Redpanda rpk skills for your AI coding assistant",
		Long: `Install Redpanda rpk skills for your AI coding assistant.

This downloads the skills published at ` + skillsRepoURL + `
and installs them into your AI coding assistant's personal skills directory,
where the assistant loads them automatically. The assistant is selected with
--provider (Default 'claude').

By default only the rpk-related skills are installed. Use --all to install
every Redpanda skill (Streaming, SQL, Connect, Cloud, etc.).

Use --skills for finer control over which skills to install. It accepts a regex
matched against the available skill names, or the keyword 'list' (print the
available skills and exit) or 'select' (interactively choose skills). When set,
--skills selects from the full catalog.`,
		Example: `
Install the rpk skills for the default provider:
  rpk generate skill

Install the rpk skills for a specific provider:
  rpk generate skill --provider claude

Install every Redpanda skill to a custom directory:
  rpk generate skill --all --dir /path/to/skills

List the available skills:
  rpk generate skill --skills list

Interactively select skills to install:
  rpk generate skill --skills select

Install skills matching a regex:
  rpk generate skill --skills 'rpk-(topic|group)'
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			prov, ok := skillProviders[provider]
			if !ok {
				out.Die("unsupported provider %q; supported providers: %s", provider, strings.Join(sortedProviderNames(), ", "))
			}
			if dir == "" {
				var err error
				dir, err = prov.defaultDir()
				out.MaybeDieErr(err)
			}

			spinner := out.NewSpinner(cmd.Context(), fmt.Sprintf("Downloading skills from github.com/%s@%s...", skillsRepo, branch), out.WithElapsedTime())
			tgz, err := downloadSkillsTarball(cmd.Context(), branch)
			if err != nil {
				spinner.Fail("Failed to download skills")
				out.MaybeDieErr(err)
			}
			catalog, err := parseSkills(bytes.NewReader(tgz))
			if err != nil {
				spinner.Fail("Failed to read skills")
				out.MaybeDieErr(err)
			}
			spinner.Success("Skills downloaded")

			match, install, err := resolveSkillMatch(skills, all, catalog.names())
			out.MaybeDieErr(err)
			if !install {
				return
			}

			installed, err := extractSkills(fs, catalog, dir, match)
			out.MaybeDieErr(err)
			if len(installed) == 0 {
				if all {
					out.Die("no skills found in github.com/%s", skillsRepo)
				}
				out.Die("no rpk skills found in github.com/%s", skillsRepo)
			}

			tw := out.NewTable("skill")
			for _, s := range installed {
				tw.Print(s)
			}
			tw.Flush()
			fmt.Printf("\nInstalled %d skill(s) into %s.\nThey are now available in %s.\n", len(installed), dir, prov.displayName)
		},
	}
	f := cmd.Flags()
	f.StringVar(&provider, "provider", defaultProvider, "AI coding assistant to install skills for (e.g. claude)")
	f.StringVar(&branch, "branch", "main", "Repository branch to install skills from")
	f.StringVar(&dir, "dir", "", "Destination directory (default is the provider's skills directory)")
	f.StringVar(&skills, "skills", "", "Skills to install: a regex matched against skill names, or the keyword 'list' or 'select'")
	f.BoolVar(&all, "all", false, "Install all Redpanda skills instead of only the rpk skills")

	cmd.MarkFlagsMutuallyExclusive("skills", "all")
	cmd.RegisterFlagCompletionFunc("provider", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return sortedProviderNames(), cobra.ShellCompDirectiveDefault
	})
	return cmd
}

// resolveSkillMatch handles the --skills flag. The 'list' and 'select' keywords
// are reserved words; any other non-empty value is treated as a regex (matched
// against the skill names), and an empty value falls back to the default
// rpk/--all filter.
//
// The returned install bool is false when the flag fully handled the request
// and there is nothing to install (the 'list' catalog was printed, or 'select'
// was canceled with no choice).
func resolveSkillMatch(skills string, all bool, available []string) (match func(name string) bool, install bool, err error) {
	switch {
	case skills == "list":
		tw := out.NewTable("skill")
		for _, s := range available {
			tw.Print(s)
		}
		tw.Flush()
		return nil, false, nil
	case skills == "select":
		chosen, err := out.PickMultiple(available, "Select the skills to install")
		if err != nil {
			return nil, false, err
		}
		if len(chosen) == 0 {
			fmt.Println("No skills selected.")
			return nil, false, nil
		}
		chosenSet := make(map[string]bool, len(chosen))
		for _, c := range chosen {
			chosenSet[c] = true
		}
		return func(name string) bool { return chosenSet[name] }, true, nil
	case skills != "":
		re, err := regexp.Compile(skills)
		if err != nil {
			return nil, false, fmt.Errorf("invalid --skills regex %q: %v", skills, err)
		}
		if len(matchSkills(available, re)) == 0 {
			return nil, false, fmt.Errorf("no skills in github.com/%s match %q; available skills: %s", skillsRepo, skills, strings.Join(available, ", "))
		}
		return re.MatchString, true, nil
	default:
		return func(name string) bool { return wantSkill(name, all) }, true, nil
	}
}

func downloadSkillsTarball(ctx context.Context, branch string) ([]byte, error) {
	url := fmt.Sprintf("https://codeload.github.com/%s/tar.gz/refs/heads/%s", skillsRepo, branch)
	cl := httpapi.NewClient(
		httpapi.HTTPClient(&http.Client{Timeout: 2 * time.Minute}),
	)
	var buf bytes.Buffer
	if err := cl.Get(ctx, url, nil, &buf); err != nil {
		return nil, fmt.Errorf("unable to download skills from %s: %w", url, err)
	}
	return buf.Bytes(), nil
}

func wantSkill(name string, all bool) bool {
	return all || name == "rpk" || strings.HasPrefix(name, "rpk-")
}

// matchSkills returns the subset of available skill names matching re.
func matchSkills(available []string, re *regexp.Regexp) []string {
	var matched []string
	for _, n := range available {
		if re.MatchString(n) {
			matched = append(matched, n)
		}
	}
	return matched
}

// walkSkillFiles iterates the regular files of the skills repository tarball,
// invoking fn for each file under "skills/<name>/<rel>". GitHub tarballs nest
// everything under a "<repo>-<branch>/" directory, which is stripped here.
func walkSkillFiles(tarGz io.Reader, fn func(name, rel string, h *tar.Header, tr *tar.Reader) error) error {
	gzr, err := gzip.NewReader(tarGz)
	if err != nil {
		return fmt.Errorf("unable to read skills archive: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("unable to read skills archive: %w", err)
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		rel := strings.TrimPrefix(path.Clean(h.Name), "/")
		parts := strings.SplitN(rel, "/", 4)
		if len(parts) < 4 || parts[1] != "skills" {
			continue
		}
		if err := fn(parts[2], parts[3], h, tr); err != nil {
			return err
		}
	}
	return nil
}

// skillFile is one regular file belonging to a skill.
type skillFile struct {
	rel  string      // path within the skill directory
	mode os.FileMode // original file mode from the archive
	data []byte
}

// skillCatalog maps a skill name to its files, decoded from the skills tarball.
type skillCatalog map[string][]skillFile

// names returns the catalog's skill names, sorted.
func (c skillCatalog) names() []string {
	result := slices.Collect(maps.Keys(c))
	slices.Sort(result)
	return result
}

// parseSkills decodes the skills repository tarball into a catalog keyed by
// skill name. The archive is decompressed once; both listing the available
// skills and installing the selected ones operate on the returned catalog.
func parseSkills(tarGz io.Reader) (skillCatalog, error) {
	catalog := make(skillCatalog)
	err := walkSkillFiles(tarGz, func(name, rel string, h *tar.Header, tr *tar.Reader) error {
		data, err := io.ReadAll(tr)
		if err != nil {
			return fmt.Errorf("unable to read %q from skills archive: %w", h.Name, err)
		}
		catalog[name] = append(catalog[name], skillFile{rel: rel, mode: h.FileInfo().Mode(), data: data})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return catalog, nil
}

// extractSkills writes the catalog skills for which match returns true under
// destDir, returning the sorted list of installed skill names.
func extractSkills(fs afero.Fs, catalog skillCatalog, destDir string, match func(name string) bool) ([]string, error) {
	var installed []string
	for _, name := range catalog.names() {
		if !match(name) {
			continue
		}
		// Clear any stale files from a prior install before rewriting.
		skillDir := filepath.Join(destDir, name)
		if err := fs.RemoveAll(skillDir); err != nil {
			return nil, fmt.Errorf("unable to remove existing skill %q: %w", name, err)
		}
		for _, f := range catalog[name] {
			dst := filepath.Join(skillDir, filepath.FromSlash(f.rel))
			if err := rpkos.ReplaceFile(fs, dst, f.data, f.mode); err != nil {
				return nil, fmt.Errorf("unable to write %q: %w", dst, err)
			}
		}
		installed = append(installed, name)
	}
	return installed, nil
}
