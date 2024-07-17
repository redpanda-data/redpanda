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

	controlplanev1beta2 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1beta2"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	container "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloudapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/rs/xid"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		set               []string
		fromRedpanda      string
		fromProfile       string
		fromCloud         string
		fromContainer     bool
		description       string
		fromCloudFlag     = "from-cloud"
		fromContainerFlag = "from-rpk-container"
	)

	cmd := &cobra.Command{
		Use:   "create [NAME]",
		Short: "Create an rpk profile",
		Long: `Create an rpk profile.

There are multiple ways to create a profile. A name must be provided if not
using --from-cloud or --from-rpk-container.

* You can use --from-redpanda to generate a new profile from an existing
  redpanda.yaml file. The special values "current" create a profile from the
  current redpanda.yaml as it is loaded within rpk.

* You can use --from-profile to generate a profile from an existing profile or
  from from a profile in a yaml file. First, the filename is checked, then an
  existing profile name is checked. The special value "current" creates a new
  profile from the existing profile with any active environment variables or
  flags applied.

* You can use --from-cloud to generate a profile from an existing cloud cluster
  id. Note that you must be logged in with 'rpk cloud login' first. The special
  value "prompt" will prompt to select a cloud cluster to create a profile for.

* You can use --from-rpk-container to generate a profile from an existing
  cluster created using 'rpk container start' command. The name is not needed
  when using this flag.

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
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			yAct, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "unable to load rpk.yaml: %v", err)
			authVir := cfg.VirtualRpkYaml().CurrentAuth()

			var name string
			if len(args) > 0 {
				name = args[0]
			}

			cloudFlag := cmd.Flags().Changed(fromCloudFlag)
			containerFlag := cmd.Flags().Changed(fromContainerFlag)
			if name == "" && !containerFlag && !cloudFlag {
				out.Die("profile name cannot be empty unless using %v or %v", fromCloudFlag, fromContainerFlag)
			}

			err = CreateFlow(cmd.Context(), fs, cfg, yAct, authVir, fromRedpanda, fromProfile, fromCloud, fromContainer, set, name, description)
			if ee := (*ProfileExistsError)(nil); errors.As(err, &ee) {
				fmt.Printf(`Unable to automatically create profile %q due to a name conflict with
an existing self-hosted profile, please rename that profile or use a different
name argument to this command.

Either:
    rpk profile use %[1]q
    rpk profile rename-to $something_else
    rpk profile create %[1]q --your-flags-here
Or:
    rpk profile create $another_name --your-flags-here
`, ee.Name)
				os.Exit(1)
			}
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().StringArrayVarP(&set, "set", "s", nil, "Create and switch to a new profile, setting profile fields with key=value pairs")
	cmd.Flags().StringVar(&fromRedpanda, "from-redpanda", "", "Create and switch to a new profile from a redpanda.yaml file")
	cmd.Flags().StringVar(&fromProfile, "from-profile", "", "Create and switch to a new profile from an existing profile or from a profile in a yaml file")
	cmd.Flags().StringVar(&fromCloud, fromCloudFlag, "", "Create and switch to a new profile generated from a Redpanda Cloud cluster ID")
	cmd.Flags().BoolVar(&fromContainer, fromContainerFlag, false, "Create and switch to a new profile generated from a running cluster created with rpk container")
	cmd.Flags().StringVarP(&description, "description", "d", "", "Optional description of the profile")

	cmd.Flags().Lookup("from-cloud").NoOptDefVal = "prompt"

	cmd.RegisterFlagCompletionFunc("set", validSetArgs)

	cmd.MarkFlagsMutuallyExclusive("from-redpanda", "from-cloud", "from-profile")

	return cmd
}

// CreateFlow runs the profile creation flow and prints what was done.
func CreateFlow(
	ctx context.Context,
	fs afero.Fs,
	cfg *config.Config,
	yAct *config.RpkYaml,
	yAuthVir *config.RpkCloudAuth,
	fromRedpanda string,
	fromProfile string,
	fromCloud string,
	fromContainer bool,
	set []string,
	name string,
	description string,
) error {
	if p := yAct.Profile(name); p != nil {
		return &ProfileExistsError{name}
	}

	var (
		p = new(config.RpkProfile) // if we do not use any `--from-flag`, we create a default empty profile
		o CloudClusterOutputs

		priorProfile = yAct.Profile(yAct.CurrentProfile)

		updatingRpkCloudProfile bool
	)
	switch {
	case fromContainer:
		if name != "" {
			return errors.New("unable to create profile: name is not allowed when using --from-rpk-container flag")
		}
		c, err := container.NewDockerClient(ctx)
		if err != nil {
			return fmt.Errorf("unable to create docker client: %v", err)
		}
		err = container.CreateProfile(fs, c, yAct) //nolint:contextcheck // No need to pass the context, the underlying functions use a context with timeout.
		if err != nil {
			return fmt.Errorf("unable to create profile from rpk container: %v", err)
		}

		// container.CreateProfile writes the yaml file and pushes the
		// new profile to be current. The code is in a separate package
		// because container start automatically creates a profile as
		// well.
		fmt.Printf("Created and switch to profile %q.\n", container.ContainerProfileName)
		fmt.Println("rpk will now talk to your locally running Redpanda container cluster.")
		return nil

	case fromCloud != "":
		var err error
		o, err = createCloudProfile(ctx, yAuthVir, cfg, fromCloud)
		if err != nil {
			if errors.Is(err, ErrNoCloudClusters) {
				fmt.Println("Your cloud account has no clusters available to select, avoiding creating a cloud profile.")
				return nil
			}
			return err
		}
		p = &o.Profile

		if name == "" {
			name = RpkCloudProfileName
			if old := yAct.Profile(name); old != nil {
				if !old.FromCloud {
					return &ProfileExistsError{name}
				}
				*old = *p // update
				p = old   // the values
				updatingRpkCloudProfile = true
			}
		}

	case fromProfile != "":
		switch {
		case fromProfile == "current":
			dup := *cfg.VirtualProfile()
			p = &dup
		default:
			raw, err := afero.ReadFile(fs, fromProfile)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("unable to read file %q: %v", fromProfile, err)
				}
				src := yAct.Profile(fromProfile)
				if src == nil {
					return fmt.Errorf("unable to find profile %q", fromProfile)
				}
				dup := *src
				p = &dup
			} else if err := yaml.Unmarshal(raw, &p); err != nil {
				return fmt.Errorf("unable to yaml decode file %q: %v", fromProfile, err)
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
				return fmt.Errorf("unable to read file %q: %v", fromRedpanda, err)
			}
			var rpyaml config.RedpandaYaml
			if err := yaml.Unmarshal(raw, &rpyaml); err != nil {
				return fmt.Errorf("unable to yaml decode file %q: %v", fromRedpanda, err)
			}
			nodeCfg = rpyaml.Rpk
		}
		p = &config.RpkProfile{
			KafkaAPI: nodeCfg.KafkaAPI,
			AdminAPI: nodeCfg.AdminAPI,
		}
	}
	if err := doSet(p, set); err != nil {
		return err
	}

	if name != "" {
		p.Name = name // name can be empty if we are creating a cloud cluster based profile
	}
	if description != "" {
		p.Description = description // p.Description could be set by cloud cluster loading; only override if the user specified
	}

	switch {
	case updatingRpkCloudProfile && priorProfile != nil && priorProfile.FromCloud && priorProfile.Name == RpkCloudProfileName:
		// * We are creating a profile for a cloud cluster
		// * The user's prior profile was the rpk-cloud profile
		// * We are updating the values of the existing current profile
		fmt.Printf("Updated profile %q to talk to cloud cluster %q.\n", RpkCloudProfileName, o.FullName())
		if yAct.CurrentProfile != RpkCloudProfileName {
			panic("invalid invariant: prior profile should have been the rpk-cloud profile")
		}

	case updatingRpkCloudProfile:
		// * We are creating a profile for a cloud cluster
		// * The user's prior profile was NOT the rpk-cloud profile
		// * We are switching to the existing rpk-cloud profile and updating the values
		fmt.Printf("Switched to existing %q profile and updated it to talk to cluster %q.\n", RpkCloudProfileName, o.FullName())
		priorAuth, currentAuth := yAct.MoveProfileToFront(&p)
		config.MaybePrintAuthSwitchMessage(priorAuth, currentAuth)

	case fromCloud != "" && name == RpkCloudProfileName:
		// * We are creating a NEW rpk-cloud profile and switching to it
		fmt.Printf("Created and switched to a new profile %q to talk to cloud cluster %q.\n", RpkCloudProfileName, o.FullName())
		priorAuth, currentAuth := yAct.PushProfile(*p)
		config.MaybePrintAuthSwitchMessage(priorAuth, currentAuth)

	default:
		// * This is not a cloud nor container profile, this is a
		//   regularly manually created profile, or duplicating an
		//   existing one. We print standard messaging.
		fmt.Printf("Created and switched to new profile %q.\n", p.Name)
		priorAuth, currentAuth := yAct.PushProfile(*p)
		config.MaybePrintAuthSwitchMessage(priorAuth, currentAuth)
	}

	if err := yAct.Write(fs); err != nil {
		return fmt.Errorf("unable to write rpk file: %v", err)
	}

	// Now that we have successfully written a profile, we build any output
	// message if this is a cloud based profile.
	if fromCloud != "" {
		msg := CloudClusterMessage(*p, o.ClusterName, o.ClusterID)
		if o.MessageSASL && p.KafkaAPI.SASL == nil {
			msg += RequiresSASLMessage()
		}
		if o.MessageMTLS && p.KafkaAPI.TLS == nil {
			msg += RequiresMTLSMessage()
		}
		fmt.Println(msg)
	}

	return nil
}

func createCloudProfile(ctx context.Context, yAuthVir *config.RpkCloudAuth, cfg *config.Config, clusterIDOrName string) (CloudClusterOutputs, error) {
	if yAuthVir == nil {
		return CloudClusterOutputs{}, errors.New("missing current cloud auth, please login with 'rpk cloud login'")
	}

	overrides := cfg.DevOverrides()
	auth0Cl := auth0.NewClient(overrides)
	expired, err := oauth.ValidateToken(yAuthVir.AuthToken, auth0Cl.Audience(), yAuthVir.ClientID)
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	if expired {
		return CloudClusterOutputs{}, errors.New("current cloud auth has expired, please re-login with 'rpk cloud login'")
	}
	cl := cloudapi.NewClient(overrides.CloudAPIURL, yAuthVir.AuthToken, httpapi.ReqTimeout(10*time.Second))

	cpCl, err := publicapi.NewControlPlaneClientSet(cfg.DevOverrides().PublicAPIURL, yAuthVir.AuthToken)
	if err != nil {
		return CloudClusterOutputs{}, fmt.Errorf("unable to create public API client: %v", err)
	}
	if clusterIDOrName == "prompt" {
		return PromptCloudClusterProfile(ctx, yAuthVir, cl, cpCl)
	}

	var (
		clusterID       string
		triedNameLookup bool
		forceNameLookup bool
	)
nameLookup:
	_, err = xid.FromString(clusterIDOrName)
	if err != nil || forceNameLookup {
		clusterID, err = clusterNameToID(ctx, cl, clusterIDOrName)
		if err != nil {
			if forceNameLookup {
				// It is possible that an API call failed, but odds are *at this point*
				// that we can query the API successfully (as we have already done so
				// multiple times), and the problem is the name does not exist.
				return CloudClusterOutputs{}, fmt.Errorf("unable to find a cluster ID nor a cluster name for %q", clusterIDOrName)
			}
			return CloudClusterOutputs{}, err
		}
		triedNameLookup = true
	} else {
		clusterID = clusterIDOrName
	}

	// When we create a cloud profile, we want the namespace name so that
	// we can print some nicer looking messages. When we finally know the
	// cluster ID, we do a final namespace lookup and map the cluster's
	// namespace UUID to the namespace name.

	vc, err := cl.VirtualCluster(ctx, clusterID)
	if err != nil { // if we fail for a vcluster, we try again for a normal cluster
		cluster, err := cpCl.ClusterForID(ctx, clusterID)
		if err != nil {
			// If the input cluster looks like an xid, we try
			// parsing it as a cluster ID. If the xid lookup fails,
			// it is POSSIBLE that the cluster name itself looks
			// like an xid. We retry from the top forcing a name
			// lookup; if that also fails, we return early above.
			if !forceNameLookup && !triedNameLookup {
				forceNameLookup = true
				goto nameLookup
			}
			return CloudClusterOutputs{}, fmt.Errorf("unable to request details for cluster %q: %w", clusterID, err)
		}
		if cluster.State != controlplanev1beta2.Cluster_STATE_READY {
			return CloudClusterOutputs{}, fmt.Errorf("selected cluster %q is not ready for profile creation yet; you may run this command again once the cluster is running", clusterID)
		}
		rg, err := cpCl.ResourceGroupForID(ctx, cluster.GetResourceGroupId())
		if err != nil {
			return CloudClusterOutputs{}, err
		}
		return fromCloudCluster(yAuthVir, rg, cluster), nil
	}
	rg, err := cpCl.ResourceGroupForID(ctx, vc.NamespaceUUID)
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	return fromVirtualCluster(yAuthVir, rg, vc), nil
}

func clusterNameToID(ctx context.Context, cl *cloudapi.Client, name string) (string, error) {
	_, nss, vcs, cs, err := cl.OrgNamespacesClusters(ctx)
	if err != nil {
		return "", fmt.Errorf("unable to request organization, namespace, or cluster details: %w", err)
	}
	candidates := findNamedCluster(name, nss, vcs, cs)

	switch len(candidates) {
	case 0:
		return "", fmt.Errorf("no cluster found with name %q", name)
	case 1:
		for _, nc := range candidates {
			if nc.IsVCluster {
				return nc.VCluster.ID, nil
			} else {
				return nc.Cluster.ID, nil
			}
		}
		panic("unreachable")
	default:
		ncs := combineClusterNames(nss, vcs, cs)
		names := ncs.names()

		idx, err := out.PickIndex(names, "Multiple clusters found with the requested name, please select one:")
		if err != nil {
			return "", err
		}
		return ncs[idx].clusterID(), nil
	}
}

// Iterates across vcs and cs and returns all clusters that match the given
// name.
func findNamedCluster(name string, nss []cloudapi.Namespace, vcs []cloudapi.VirtualCluster, cs []cloudapi.Cluster) map[string]cloudapi.NamespacedCluster {
	ret := make(map[string]cloudapi.NamespacedCluster)
	namespaceIDs := make(map[string]cloudapi.Namespace, len(nss))
	for _, ns := range nss {
		namespaceIDs[ns.ID] = ns
	}
	for _, vc := range vcs {
		if name != vc.Name && name != fmt.Sprintf("%s/%s", namespaceIDs[vc.NamespaceUUID].Name, vc.Name) {
			continue
		}
		ns := namespaceIDs[vc.NamespaceUUID]
		ret[vc.Name] = cloudapi.NamespacedCluster{
			Namespace:  ns,
			VCluster:   vc,
			IsVCluster: true,
		}
	}
	for _, c := range cs {
		if name != c.Name && name != fmt.Sprintf("%s/%s", namespaceIDs[c.NamespaceUUID].Name, c.Name) {
			continue
		}
		ns := namespaceIDs[c.NamespaceUUID]
		ret[c.Name] = cloudapi.NamespacedCluster{
			Namespace: ns,
			Cluster:   c,
		}
	}
	return ret
}

// fromCloudCluster returns an rpk profile from a cloud cluster, as well
// as if the cluster requires mtls or sasl.
func fromCloudCluster(yAuth *config.RpkCloudAuth, rg *controlplanev1beta2.ResourceGroup, c *controlplanev1beta2.Cluster) CloudClusterOutputs {
	p := config.RpkProfile{
		Name:      c.Name,
		FromCloud: true,
		CloudCluster: config.RpkCloudCluster{
			ResourceGroup: rg.Name,
			ClusterID:     c.Id,
			ClusterName:   c.Name,
			AuthOrgID:     yAuth.OrgID,
			AuthKind:      yAuth.Kind,
			ClusterType:   c.Type.String(),
		},
	}
	if c.DataplaneApi != nil {
		p.CloudCluster.ClusterURL = c.DataplaneApi.Url
	}
	var isMTLS bool
	if c.KafkaApi != nil {
		p.KafkaAPI.Brokers = c.KafkaApi.SeedBrokers
		if mtls := c.KafkaApi.Mtls; mtls != nil {
			p.KafkaAPI.TLS = new(config.TLS)
			isMTLS = mtls.Enabled
		}
	}
	return CloudClusterOutputs{
		Profile:           p,
		ResourceGroupName: rg.Name,
		ClusterName:       c.Name,
		ClusterID:         c.Id,
		MessageMTLS:       isMTLS,
		MessageSASL:       true,
	}
}

func fromVirtualCluster(yAuth *config.RpkCloudAuth, rg *controlplanev1beta2.ResourceGroup, vc cloudapi.VirtualCluster) CloudClusterOutputs {
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
		CloudCluster: config.RpkCloudCluster{
			ResourceGroup: rg.Name,
			ClusterID:     vc.ID,
			ClusterName:   vc.Name,
			AuthOrgID:     yAuth.OrgID,
			AuthKind:      yAuth.Kind,
			ClusterType:   publicapi.ServerlessClusterType, // Virtual clusters do not include a type in the response yet.
		},
	}

	return CloudClusterOutputs{
		Profile:           p,
		ResourceGroupName: rg.Name,
		ClusterName:       vc.Name,
		ClusterID:         vc.ID,
		MessageMTLS:       false, // we do not need to print any required message; we generate the config in full
		MessageSASL:       false, // same
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
  Web UI: https://cloud.redpanda.com/clusters/%s/overview
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

// CloudClusterOutputs contains outputs from a cloud based profile.
type CloudClusterOutputs struct {
	Profile           config.RpkProfile
	ResourceGroupName string
	ClusterID         string
	ClusterName       string
	MessageMTLS       bool
	MessageSASL       bool
}

// FullName Duplicates RpkCloudProfile.FullName (easier for now).
func (o CloudClusterOutputs) FullName() string {
	return fmt.Sprintf("%s/%s", o.ResourceGroupName, o.ClusterName)
}

// PromptCloudClusterProfile returns a profile for the cluster selected by the
// user. If their cloud account has only one cluster, a profile is created for
// it automatically. This returns ErrNoCloudClusters if the user has no cloud
// clusters.
func PromptCloudClusterProfile(ctx context.Context, yAuth *config.RpkCloudAuth, cl *cloudapi.Client, cpCl *publicapi.ControlPlaneClientSet) (CloudClusterOutputs, error) {
	org, nss, vcs, cs, err := cl.OrgNamespacesClusters(ctx)
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	if len(cs) == 0 && len(vcs) == 0 {
		return CloudClusterOutputs{}, ErrNoCloudClusters
	}

	// Always prompt, even if there is only one option.
	ncs := combineClusterNames(nss, vcs, cs)
	names := ncs.names()
	if len(names) == 0 {
		return CloudClusterOutputs{}, ErrNoCloudClusters
	}
	idx, err := out.PickIndex(names, "Which cloud resource-group/cluster would you like to talk to?")
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	selected := ncs[idx]

	var o CloudClusterOutputs
	// We have a cluster selected, but the list response does not return
	// all information we need. We need to now directly request this
	// cluster's information.
	if selected.c != nil {
		cluster, err := cpCl.ClusterForID(ctx, selected.c.ID)
		if err != nil {
			return CloudClusterOutputs{}, err
		}
		rg, err := cpCl.ResourceGroupForID(ctx, cluster.GetResourceGroupId())
		if err != nil {
			return CloudClusterOutputs{}, err
		}
		o = fromCloudCluster(yAuth, rg, cluster)
	} else {
		vc, err := cl.VirtualCluster(ctx, selected.vc.ID)
		if err != nil {
			return CloudClusterOutputs{}, fmt.Errorf("unable to get cluster %q information: %w", vc.ID, err)
		}
		rg, err := cpCl.ResourceGroupForID(ctx, vc.NamespaceUUID)
		if err != nil {
			return CloudClusterOutputs{}, err
		}
		o = fromVirtualCluster(yAuth, rg, vc)
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

func (nc *nameAndCluster) clusterID() string {
	if nc.c != nil {
		return nc.c.ID
	}
	return nc.vc.ID
}

type namesAndClusters []nameAndCluster

func (ncs namesAndClusters) names() []string {
	var ret []string
	for _, nc := range ncs {
		ret = append(ret, nc.name)
	}
	return ret
}

// combineClusterNames combines the names of Virtual Clusters and Clusters,
// sorted alphabetically, and returns a list of nameAndCluster structs
// representing the combined clusters (VClusters first, then Clusters).
func combineClusterNames(nss cloudapi.Namespaces, vcs []cloudapi.VirtualCluster, cs []cloudapi.Cluster) namesAndClusters {
	nsIDToName := make(map[string]string, len(nss))
	for _, n := range nss {
		nsIDToName[n.ID] = n.Name
	}

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
		if strings.ToLower(c.State) != cloudapi.ClusterStateReady {
			continue
		}
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
