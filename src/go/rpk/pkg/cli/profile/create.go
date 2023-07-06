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
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloudapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		set          []string
		fromRedpanda string
		fromProfile  string
		fromCloud    string
		description  string
	)

	cmd := &cobra.Command{
		Use:   "create [NAME]",
		Short: "Create an rpk profile",
		Long: `Create an rpk profile.

There are multiple ways to create a profile. If no name is provided, the
name "default" is used.

* You can use --from-redpanda to generate a new profile from an existing
  redpanda.yaml file. The special values "current" create a profile from the
  current redpanda.yaml as it is loaded within rpk.

* You can use --from-profile to generate a profile from an existing profile or
  from from a profile in a yaml file. First, the filename is checked, then an
  existing profile name is checked. The special value "current" creates a new
  profile from the existing profile.

* You can use --from-cloud to generate a profile from an existing cloud cluster
  id. Note that you must be logged in with 'rpk cloud login' first.

* You can use --set key=value to directly set fields. The key can either be
  the name of a -X flag or the path to the field in the profile's YAML format.
  For example, using --set tls.enabled=true OR --set kafka_api.tls.enabled=true
  is equivalent. The former corresponds to the -X flag tls.enabled, while the
  latter corresponds to the path kafka_api.tls.enabled in the profile's YAML.

The --set flag is always applied last and can be used to set additional fields
in tandem with --from-redpanda or --from-cloud.

The --set flag supports autocompletion, suggesting the -X key format. If you
begin writing a YAML path, the flag will suggest the rest of the path.

It is recommended to always use the --description flag; the description is
printed in the output of 'rpk profile list'.

rpk always switches to the newly created profile.
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "unable to load rpk.yaml: %v", err)

			if len(args) == 0 {
				args = append(args, "default")
			}
			name := args[0]
			if name == "" {
				out.Die("profile name cannot be empty")
			}

			if (fromCloud != "" && fromRedpanda != "") || (fromCloud != "" && fromProfile != "") || (fromRedpanda != "" && fromProfile != "") {
				out.Die("can only use one of --from-cloud, --from-redpanda, or --from-profile")
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			cloudMTLS, cloudSASL, err := createCtx(ctx, fs, y, cfg, fromRedpanda, fromProfile, fromCloud, set, name, description)
			out.MaybeDieErr(err)

			fmt.Printf("Created and switched to new profile %q.\n", name)

			if cloudMTLS {
				fmt.Println(RequiresMTLSMessage())
			}
			if cloudSASL {
				fmt.Println(RequiresSASLMessage())
			}
		},
	}

	cmd.Flags().StringSliceVarP(&set, "set", "s", nil, "Create and switch to a new profile, setting profile fields with key=value pairs")
	cmd.Flags().StringVar(&fromRedpanda, "from-redpanda", "", "Create and switch to a new profile from a redpanda.yaml file")
	cmd.Flags().StringVar(&fromProfile, "from-profile", "", "Create and switch to a new profile from an existing profile or from a profile in a yaml file")
	cmd.Flags().StringVar(&fromCloud, "from-cloud", "", "Create and switch to a new profile generated from a Redpanda Cloud cluster ID")
	cmd.Flags().StringVarP(&description, "description", "d", "", "Optional description of the profile")

	cmd.Flags().Lookup("from-cloud").NoOptDefVal = "prompt"

	cmd.RegisterFlagCompletionFunc("set", validSetArgs)

	return cmd
}

// This returns whether the command should print cloud mTLS or SASL messages.
func createCtx(
	ctx context.Context,
	fs afero.Fs,
	y *config.RpkYaml,
	cfg *config.Config,
	fromRedpanda string,
	fromProfile string,
	fromCloud string,
	set []string,
	name string,
	description string,
) (cloudMTLS, cloudSASL bool, err error) {
	if p := y.Profile(name); p != nil {
		return false, false, fmt.Errorf("profile %q already exists", name)
	}

	var p config.RpkProfile
	switch {
	case fromCloud != "":
		var err error
		p, cloudMTLS, cloudSASL, err = createCloudProfile(ctx, y, cfg, fromCloud)
		if err != nil {
			return false, false, err
		}

	case fromProfile != "":
		switch {
		case fromProfile == "current":
			p = *cfg.VirtualProfile()
		default:
			raw, err := afero.ReadFile(fs, fromProfile)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return false, false, fmt.Errorf("unable to read file %q: %v", fromProfile, err)
				}
				y, err := cfg.ActualRpkYamlOrEmpty()
				if err != nil {
					return false, false, fmt.Errorf("file %q does not exist, and we cannot read rpk.yaml: %v", fromProfile, err)
				}
				src := y.Profile(fromProfile)
				if src == nil {
					return false, false, fmt.Errorf("unable to find profile %q", fromProfile)
				}
				p = *src
			} else if err := yaml.Unmarshal(raw, &p); err != nil {
				return false, false, fmt.Errorf("unable to yaml decode file %q: %v", fromProfile, err)
			}
		}

	case fromRedpanda != "":
		var nodeCfg config.RpkNodeConfig
		switch {
		case fromRedpanda == "current":
			nodeCfg = cfg.VirtualRedpandaYaml().Rpk
		default:
			raw, err := afero.ReadFile(fs, fromRedpanda)
			if err != nil {
				return false, false, fmt.Errorf("unable to read file %q: %v", fromRedpanda, err)
			}
			var rpyaml config.RedpandaYaml
			if err := yaml.Unmarshal(raw, &rpyaml); err != nil {
				return false, false, fmt.Errorf("unable to yaml decode file %q: %v", fromRedpanda, err)
			}
			nodeCfg = rpyaml.Rpk
		}
		p = config.RpkProfile{
			KafkaAPI: nodeCfg.KafkaAPI,
			AdminAPI: nodeCfg.AdminAPI,
		}
	}
	if err := doSet(&p, set); err != nil {
		return false, false, err
	}
	if cloudSASL && p.KafkaAPI.SASL != nil {
		cloudSASL = false
	}

	p.Name = name
	p.Description = description
	y.CurrentProfile = y.PushProfile(p)
	if err := y.Write(fs); err != nil {
		return false, false, fmt.Errorf("unable to write rpk file: %v", err)
	}
	return
}

func createCloudProfile(ctx context.Context, y *config.RpkYaml, cfg *config.Config, clusterID string) (p config.RpkProfile, cloudMTLS, cloudSASL bool, err error) {
	a := y.Auth(y.CurrentCloudAuth)
	if a == nil {
		return p, false, false, fmt.Errorf("missing auth for current_cloud_auth %q", y.CurrentCloudAuth)
	}

	overrides := cfg.DevOverrides()
	auth0Cl := auth0.NewClient(overrides)
	expired, err := oauth.ValidateToken(a.AuthToken, auth0Cl.Audience(), a.ClientID)
	if err != nil {
		return p, false, false, err
	}
	if expired {
		return p, false, false, fmt.Errorf("token for %q has expired, please login again", y.CurrentCloudAuth)
	}
	cl := cloudapi.NewClient(overrides.CloudAPIURL, a.AuthToken)

	if clusterID == "prompt" {
		return PromptCloudClusterProfile(ctx, cl)
	}

	c, err := cl.Cluster(ctx, clusterID)
	if err != nil {
		return p, false, false, fmt.Errorf("unable to request details for cluster %q: %w", clusterID, err)
	}
	p, cloudMTLS, cloudSASL = fromCloudCluster(c)
	return p, cloudMTLS, cloudSASL, nil
}

// fromCloudCluster returns an rpk profile from a cloud cluster, as well
// as if the cluster requires mtls or sasl.
func fromCloudCluster(c cloudapi.Cluster) (p config.RpkProfile, isMTLS, isSASL bool) {
	p = config.RpkProfile{
		Name:      c.Name,
		FromCloud: true,
	}
	p.KafkaAPI.Brokers = c.Status.Listeners.Kafka.Default.URLs
	if l := c.Spec.KafkaListeners.Listeners; len(l) > 0 {
		if l[0].TLS != nil {
			p.KafkaAPI.TLS = new(config.TLS)
			isMTLS = l[0].TLS.RequireClientAuth
		}
		isSASL = l[0].SASL != nil
	}
	return p, isMTLS, isSASL
}

func fromVirtualCluster(vc cloudapi.VirtualCluster) (p config.RpkProfile, isMTLS, isSASL bool) {
	p = config.RpkProfile{
		Name:      vc.Name,
		FromCloud: true,
		KafkaAPI: config.RpkKafkaAPI{
			Brokers: vc.Status.Listeners.SeedAddresses,
			TLS:     new(config.TLS),
			SASL: &config.SASL{
				Mechanism: adminapi.CloudOIDC,
			},
		},
	}
	return p, false, false // we do not need to print any required message; we generate the config in full
}

// RequiresMTLSMessage returns the message to print if the cluster requires
// mTLS.
func RequiresMTLSMessage() string {
	return `
This cluster uses mTLS. Please ensure you have client certificates on your
machine an then run
    rpk profile set tls.ca /path/to/ca.pem
    rpk profile set tls.cert /path/to/cert.pem
    rpk profile set tls.key /path/to/key.pem`
}

// RequiresSASLMessage returns the message to print if the cluster requires
// SASL.
func RequiresSASLMessage() string {
	return `
If your cluster requires SASL, generate SASL credentials in the UI and then set
them in rpk with
    rpk profile set user {sasl_username}
    rpk profile set pass {sasl_password}`
}

// ErrNoCloudClusters is returned when the user has no cloud clusters.
var ErrNoCloudClusters = errors.New("no clusters found")

// PromptCloudClusterProfile returns a profile for the cluster selected by the
// user. If their cloud account has only one cluster, a profile is created for
// it automatically. This returns ErrNoCloudClusters if the user has no cloud
// clusters.
func PromptCloudClusterProfile(ctx context.Context, cl *cloudapi.Client) (p config.RpkProfile, requiresMTLS, requiresSASL bool, err error) {
	vcs, cs, err := clusterList(ctx, cl)
	if err != nil {
		return p, false, false, err
	}
	if len(cs) == 0 && len(vcs) == 0 {
		return p, false, false, ErrNoCloudClusters
	}

	// If there is just 1 cluster/virtual-cluster we go ahead and select that.
	var selected nameAndCluster
	if len(cs) == 1 && len(vcs) == 0 {
		selected = nameAndCluster{
			name: cs[0].Name,
			c:    &cs[0],
		}
	} else if len(vcs) == 1 && len(cs) == 0 {
		selected = nameAndCluster{
			name: vcs[0].Name,
			vc:   &vcs[0],
		}
	} else {
		ns, err := cl.Namespaces(ctx)
		if err != nil {
			return p, false, false, fmt.Errorf("unable to get list of namespaces: %w", err)
		}
		nsIDToName := make(map[string]string, len(ns))
		for _, n := range ns {
			nsIDToName[n.ID] = n.Name
		}
		nameAndCs := combineClusterNames(vcs, cs, nsIDToName)

		var names []string
		for _, nc := range nameAndCs {
			names = append(names, nc.name)
		}
		idx, err := out.PickIndex(names, "Which cloud namespace/cluster would you like to create a profile for?")
		if err != nil {
			return p, false, false, err
		}
		selected = nameAndCs[idx]
	}

	// We have a cluster selected, but the list response does not return
	// all information we need. We need to now directly request this
	// cluster's information.
	if selected.c != nil {
		c, err := cl.Cluster(ctx, selected.c.ID)
		if err != nil {
			return p, false, false, fmt.Errorf("unable to get cluster %q information: %w", c.ID, err)
		}
		p, requiresMTLS, requiresSASL = fromCloudCluster(c)
	} else {
		c, err := cl.VirtualCluster(ctx, selected.vc.ID)
		if err != nil {
			return p, false, false, fmt.Errorf("unable to get cluster %q information: %w", c.ID, err)
		}
		p, requiresMTLS, requiresSASL = fromVirtualCluster(c)
	}
	return p, requiresMTLS, requiresSASL, nil
}

// nameAndCluster describes a cluster name in the form of
// <namespace>/<cluster-name> and the cluster type (virtual, normal).
type nameAndCluster struct {
	name string
	c    *cloudapi.Cluster
	vc   *cloudapi.VirtualCluster
}

func clusterList(ctx context.Context, cl *cloudapi.Client) (vcs []cloudapi.VirtualCluster, cs []cloudapi.Cluster, err error) {
	g, egCtx := errgroup.WithContext(ctx)
	g.Go(func() (rerr error) {
		vcs, rerr = cl.VirtualClusters(egCtx)
		if rerr != nil {
			return fmt.Errorf("unable to get the list of virtual clusters: %v", rerr)
		}
		return nil
	})
	g.Go(func() (rerr error) {
		cs, rerr = cl.Clusters(egCtx)
		if rerr != nil {
			return fmt.Errorf("unable to get the list of clusters: %v", rerr)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	return
}

// combineClusterNames combines the names of Virtual Clusters and Clusters,
// sorted alphabetically, and returns a list of nameAndCluster structs
// representing the combined clusters (VClusters first, then Clusters).
func combineClusterNames(vcs []cloudapi.VirtualCluster, cs []cloudapi.Cluster, nsIDToName map[string]string) []nameAndCluster {
	// First we display the Virtual Clusters
	var vNameAndCs []nameAndCluster
	for _, vc := range vcs {
		vc := vc
		if strings.ToLower(vc.State) != cloudapi.ClusterStateReady {
			continue
		}
		vNameAndCs = append(vNameAndCs, nameAndCluster{
			name: fmt.Sprintf("%s/%s", nsIDToName[vc.NamespaceUUID], vc.Name),
			vc:   &vc,
		})
	}
	sort.Slice(vNameAndCs, func(i, j int) bool {
		return vNameAndCs[i].name < vNameAndCs[j].name
	})

	// Then we append the cluster names
	var nameAndCs []nameAndCluster
	for _, c := range cs {
		c := c
		nameAndCs = append(nameAndCs, nameAndCluster{
			name: fmt.Sprintf("%s/%s", nsIDToName[c.NamespaceUUID], c.Name),
			c:    &c,
		})
	}
	sort.Slice(nameAndCs, func(i, j int) bool {
		return nameAndCs[i].name < nameAndCs[j].name
	})

	return append(vNameAndCs, nameAndCs...)
}
