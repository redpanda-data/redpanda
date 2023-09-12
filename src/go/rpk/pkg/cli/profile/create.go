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

There are multiple ways to create a profile. A name must be provided if not
using --from-cloud.

* You can use --from-redpanda to generate a new profile from an existing
  redpanda.yaml file. The special values "current" create a profile from the
  current redpanda.yaml as it is loaded within rpk.

* You can use --from-profile to generate a profile from an existing profile or
  from from a profile in a yaml file. First, the filename is checked, then an
  existing profile name is checked. The special value "current" creates a new
  profile from the existing profile.

* You can use --from-cloud to generate a profile from an existing cloud cluster
  id. Note that you must be logged in with 'rpk cloud login' first. The special
  value "prompt" will prompt to select a cloud cluster to create a profile for.

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

			var name string
			if len(args) > 0 {
				name = args[0]
			}
			if name == "" && fromCloud == "" {
				out.Die("profile name cannot be empty unless using --from-cloud")
			}

			if (fromCloud != "" && fromRedpanda != "") || (fromCloud != "" && fromProfile != "") || (fromRedpanda != "" && fromProfile != "") {
				out.Die("can only use one of --from-cloud, --from-redpanda, or --from-profile")
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			name, msg, err := createProfile(ctx, fs, y, cfg, fromRedpanda, fromProfile, fromCloud, set, name, description)
			out.MaybeDieErr(err)

			fmt.Printf("Created and switched to new profile %q.\n", name)
			if msg != "" {
				fmt.Print(msg)
			}
		},
	}

	cmd.Flags().StringArrayVarP(&set, "set", "s", nil, "Create and switch to a new profile, setting profile fields with key=value pairs")
	cmd.Flags().StringVar(&fromRedpanda, "from-redpanda", "", "Create and switch to a new profile from a redpanda.yaml file")
	cmd.Flags().StringVar(&fromProfile, "from-profile", "", "Create and switch to a new profile from an existing profile or from a profile in a yaml file")
	cmd.Flags().StringVar(&fromCloud, "from-cloud", "", "Create and switch to a new profile generated from a Redpanda Cloud cluster ID")
	cmd.Flags().StringVarP(&description, "description", "d", "", "Optional description of the profile")

	cmd.Flags().Lookup("from-cloud").NoOptDefVal = "prompt"

	cmd.RegisterFlagCompletionFunc("set", validSetArgs)

	return cmd
}

// This returns whether the command should print cloud mTLS or SASL messages.
func createProfile(
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
) (finalName string, msg string, err error) {
	if p := y.Profile(name); p != nil {
		return "", "", fmt.Errorf("profile %q already exists", name)
	}

	var (
		p config.RpkProfile
		o CloudClusterOutputs
	)
	switch {
	case fromCloud != "":
		var err error
		o, err = createCloudProfile(ctx, y, cfg, fromCloud)
		if err != nil {
			return "", "", err
		}
		p = o.Profile
		if name == "" {
			if p := y.Profile(p.Name); p != nil {
				return "", "", fmt.Errorf("profile %q already exists, please try again and specify a new name", p.Name)
			}
		}

	case fromProfile != "":
		switch {
		case fromProfile == "current":
			p = *cfg.VirtualProfile()
		default:
			raw, err := afero.ReadFile(fs, fromProfile)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return "", "", fmt.Errorf("unable to read file %q: %v", fromProfile, err)
				}
				y, err := cfg.ActualRpkYamlOrEmpty()
				if err != nil {
					return "", "", fmt.Errorf("file %q does not exist, and we cannot read rpk.yaml: %v", fromProfile, err)
				}
				src := y.Profile(fromProfile)
				if src == nil {
					return "", "", fmt.Errorf("unable to find profile %q", fromProfile)
				}
				p = *src
			} else if err := yaml.Unmarshal(raw, &p); err != nil {
				return "", "", fmt.Errorf("unable to yaml decode file %q: %v", fromProfile, err)
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
				return "", "", fmt.Errorf("unable to read file %q: %v", fromRedpanda, err)
			}
			var rpyaml config.RedpandaYaml
			if err := yaml.Unmarshal(raw, &rpyaml); err != nil {
				return "", "", fmt.Errorf("unable to yaml decode file %q: %v", fromRedpanda, err)
			}
			nodeCfg = rpyaml.Rpk
		}
		p = config.RpkProfile{
			KafkaAPI: nodeCfg.KafkaAPI,
			AdminAPI: nodeCfg.AdminAPI,
		}
	}
	if err := doSet(&p, set); err != nil {
		return "", "", err
	}

	if name != "" {
		p.Name = name // name can be empty if we are creating a cloud cluster based profile
	}
	if description != "" {
		p.Description = description // p.Description could be set by cloud cluster loading; only override if the user specified
	}
	y.CurrentProfile = y.PushProfile(p)
	if err := y.Write(fs); err != nil {
		return "", "", fmt.Errorf("unable to write rpk file: %v", err)
	}

	// Now that we have successfully written a profile, we build any output
	// message if this is a cloud based profile.
	if fromCloud != "" {
		msg += CloudClusterMessage(p, o.ClusterName, o.ClusterID)
		if o.MessageSASL && p.KafkaAPI.SASL == nil {
			msg += RequiresSASLMessage()
		}
		if o.MessageMTLS && p.KafkaAPI.TLS == nil {
			msg += RequiresMTLSMessage()
		}
	}

	return y.CurrentProfile, msg, nil
}

func createCloudProfile(ctx context.Context, y *config.RpkYaml, cfg *config.Config, clusterID string) (CloudClusterOutputs, error) {
	a := y.Auth(y.CurrentCloudAuth)
	if a == nil {
		return CloudClusterOutputs{}, fmt.Errorf("missing auth for current_cloud_auth %q", y.CurrentCloudAuth)
	}

	overrides := cfg.DevOverrides()
	auth0Cl := auth0.NewClient(overrides)
	expired, err := oauth.ValidateToken(a.AuthToken, auth0Cl.Audience(), a.ClientID)
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	if expired {
		return CloudClusterOutputs{}, fmt.Errorf("token for %q has expired, please login again", y.CurrentCloudAuth)
	}
	cl := cloudapi.NewClient(overrides.CloudAPIURL, a.AuthToken)

	if clusterID == "prompt" {
		return PromptCloudClusterProfile(ctx, cl)
	}

	vc, err := cl.VirtualCluster(ctx, clusterID)
	if err != nil { // if we fail for a vcluster, we try again for a normal cluster
		c, err := cl.Cluster(ctx, clusterID)
		if err != nil {
			return CloudClusterOutputs{}, fmt.Errorf("unable to request details for cluster %q: %w", clusterID, err)
		}
		return fromCloudCluster(c), nil
	}
	return fromVirtualCluster(vc, ""), nil // we do not print the serverless hello world when creating a profile directly
}

// fromCloudCluster returns an rpk profile from a cloud cluster, as well
// as if the cluster requires mtls or sasl.
func fromCloudCluster(c cloudapi.Cluster) CloudClusterOutputs {
	p := config.RpkProfile{
		Name:      c.Name,
		FromCloud: true,
	}
	p.KafkaAPI.Brokers = c.Status.Listeners.Kafka.Default.URLs
	var isMTLS, isSASL bool
	if l := c.Spec.KafkaListeners.Listeners; len(l) > 0 {
		if l[0].TLS != nil {
			p.KafkaAPI.TLS = new(config.TLS)
			isMTLS = l[0].TLS.RequireClientAuth
		}
		isSASL = l[0].SASL != nil
	}
	return CloudClusterOutputs{
		Profile:     p,
		ClusterName: c.Name,
		ClusterID:   c.ID,
		MessageMTLS: isMTLS,
		MessageSASL: isSASL,
	}
}

func fromVirtualCluster(vc cloudapi.VirtualCluster, selectedNamespaceCluster string) CloudClusterOutputs {
	p := config.RpkProfile{
		Name:      vc.Name,
		FromCloud: true,
		KafkaAPI: config.RpkKafkaAPI{
			Brokers: vc.Status.Listeners.SeedAddresses,
			TLS:     new(config.TLS),
			SASL: &config.SASL{
				Mechanism: adminapi.CloudOIDC,
			},
		},
		AdminAPI: config.RpkAdminAPI{
			Addresses: []string{vc.Status.Listeners.ConsoleURL},
			TLS:       new(config.TLS),
		},
	}

	return CloudClusterOutputs{
		Profile:           p,
		ClusterName:       vc.Name,
		ClusterID:         vc.ID,
		MessageMTLS:       false, // we do not need to print any required message; we generate the config in full
		MessageSASL:       false, // same
		IsServerlessHello: serverlessHelloNamespaceCluster == selectedNamespaceCluster,
	}
}

const (
	serverlessHelloTopic            = "hello-world"
	serverlessHelloNamespaceCluster = "default/" + serverlessHelloTopic
)

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
    rpk profile set pass {sasl_password}
`
}

// CloudClusterMessage returns details to always print for cloud clusters.
func CloudClusterMessage(p config.RpkProfile, clusterName, clusterID string) string {
	return fmt.Sprintf(`
Cluster %s
  Web UI: https://cloudv2.redpanda.com/clusters/%s/overview
  Redpanda Seed Brokers: [%s]
`, clusterName, clusterID, strings.Join(p.KafkaAPI.Brokers, ", "))
}

// ServerlessHelloMessage returns the message to print if the cluster
// is the serverless hello world cluster.
func ServerlessHelloMessage() string {
	return fmt.Sprintf(`
Consume messages from the %[1]s topic as a guide for your next steps:
  rpk topic consume %[1]s -o :end -f '%%v\n'
`, serverlessHelloTopic)
}

// ErrNoCloudClusters is returned when the user has no cloud clusters.
var ErrNoCloudClusters = errors.New("no clusters found")

// CloudClusterOutputs contains outputs from a cloud based profile.
type CloudClusterOutputs struct {
	Profile           config.RpkProfile
	ClusterID         string
	ClusterName       string
	MessageMTLS       bool
	MessageSASL       bool
	IsServerlessHello bool
}

// PromptCloudClusterProfile returns a profile for the cluster selected by the
// user. If their cloud account has only one cluster, a profile is created for
// it automatically. This returns ErrNoCloudClusters if the user has no cloud
// clusters.
func PromptCloudClusterProfile(ctx context.Context, cl *cloudapi.Client) (CloudClusterOutputs, error) {
	// We get the org name asynchronously because this is a slow request --
	// doing it while the user does a slow operation of selecting a cluster
	// hides the delay.
	//
	// We also get namespaces immediately for two reasons: (1) we always
	// want the namespace name, and (2) we might need a list of all
	// namespaces.
	var (
		org     cloudapi.Organization
		orgErr  error
		orgDone = make(chan struct{})

		nss     []cloudapi.Namespace
		nssErr  error
		nssDone = make(chan struct{})
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer close(orgDone)
		org, orgErr = cl.Organization(ctx)
	}()
	go func() {
		defer close(nssDone)
		nss, nssErr = cl.Namespaces(ctx)
	}()

	vcs, cs, err := clusterList(ctx, cl)
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	if len(cs) == 0 && len(vcs) == 0 {
		return CloudClusterOutputs{}, ErrNoCloudClusters
	}

	<-nssDone
	if nssErr != nil {
		return CloudClusterOutputs{}, fmt.Errorf("unable to get list of namespaces: %w", nssErr)
	}

	findNamespace := func(id string) string {
		for _, n := range nss {
			if n.ID == id {
				return n.Name
			}
		}
		return ""
	}

	// If there is just 1 cluster/virtual-cluster we go ahead and select that.
	var selected nameAndCluster
	if len(cs) == 1 && len(vcs) == 0 {
		selected = nameAndCluster{
			name: fmt.Sprintf("%s/%s", findNamespace(cs[0].NamespaceUUID), cs[0].Name),
			c:    &cs[0],
		}
	} else if len(vcs) == 1 && len(cs) == 0 {
		selected = nameAndCluster{
			name: fmt.Sprintf("%s/%s", findNamespace(vcs[0].NamespaceUUID), vcs[0].Name),
			vc:   &vcs[0],
		}
	} else {
		nsIDToName := make(map[string]string, len(nss))
		for _, n := range nss {
			nsIDToName[n.ID] = n.Name
		}
		nameAndCs := combineClusterNames(vcs, cs, nsIDToName)

		var names []string
		for _, nc := range nameAndCs {
			names = append(names, nc.name)
		}
		idx, err := out.PickIndex(names, "Which cloud namespace/cluster would you like to create a profile for?")
		if err != nil {
			return CloudClusterOutputs{}, err
		}
		selected = nameAndCs[idx]
	}

	<-orgDone
	if orgErr != nil {
		return CloudClusterOutputs{}, fmt.Errorf("unable to get organization: %w", orgErr)
	}

	var o CloudClusterOutputs
	// We have a cluster selected, but the list response does not return
	// all information we need. We need to now directly request this
	// cluster's information.
	if selected.c != nil {
		c, err := cl.Cluster(ctx, selected.c.ID)
		if err != nil {
			return CloudClusterOutputs{}, fmt.Errorf("unable to get cluster %q information: %w", c.ID, err)
		}
		o = fromCloudCluster(c)
	} else {
		c, err := cl.VirtualCluster(ctx, selected.vc.ID)
		if err != nil {
			return CloudClusterOutputs{}, fmt.Errorf("unable to get cluster %q information: %w", c.ID, err)
		}
		o = fromVirtualCluster(c, selected.name)
	}
	o.Profile.Description = fmt.Sprintf("%s %q", org.Name, selected.name)
	return o, nil
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
