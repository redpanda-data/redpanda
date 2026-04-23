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

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	container "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/containerutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/authtoken"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/rs/xid"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	fromCloudFlag     = "from-cloud"
	fromContainerFlag = "from-rpk-container"
)

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		set               []string
		fromRedpanda      string
		fromProfile       string
		fromCloud         string
		fromContainer     bool
		description       string
		serverlessNetwork string
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
  For serverless clusters that support both public and private networking, you
  will be prompted to select a network type unless you specify --serverless-network.
  To avoid prompts in automation, explicitly set --serverless-network to 'public'
  or 'private'.

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
		DisableFlagParsing: true, // Manually parse for the custom --from-cloud flag.
		Run: func(cmd *cobra.Command, rawArgs []string) {
			args, err := parseCreateFlags(cmd, rawArgs)
			out.MaybeDieErr(err)
			if cmd.Flags().Changed("help") {
				cmd.Help()
				return
			}
			if len(args) > 1 {
				out.Die("too many arguments, expected at most one profile name, got %d", len(args))
			}

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

			err = CreateFlow(cmd.Context(), fs, cfg, yAct, authVir, fromRedpanda, fromProfile, fromCloud, fromContainer, set, name, description, serverlessNetwork)
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
	cmd.Flags().StringVar(&serverlessNetwork, "serverless-network", "", "Networking type for serverless clusters: 'public' or 'private' (if not specified, will prompt if both are available)")

	cmd.Flags().Lookup(fromCloudFlag).NoOptDefVal = "prompt"

	cmd.RegisterFlagCompletionFunc("set", validSetArgs)
	cmd.RegisterFlagCompletionFunc("serverless-network", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"public", "private"}, cobra.ShellCompDirectiveNoFileComp
	})

	cmd.MarkFlagsMutuallyExclusive("from-redpanda", "from-cloud", "from-profile")

	return cmd
}

// parseCreateFlags parses the flags for the create command. Fixing the
// `--from-cloud` flag if it is set, and returns the remaining arguments for
// the command execution.
func parseCreateFlags(cmd *cobra.Command, args []string) ([]string, error) {
	args = fixFromCloudArgs(args)
	f := cmd.Flags()
	err := f.Parse(args)
	if err != nil {
		return nil, err
	}
	argsLeft, _ := cobraext.StripFlagset(args, f)
	return argsLeft, nil
}

// fixFromCloudArgs is a hack to 'fix' the `--from-cloud` when is set.
// We want to ensure that:
//  1. If the user provided a value, use it (--from-cloud value)
//     or (--from-cloud=value).
//  2. If the user set `--from-cloud` without a value, we want to leave it
//     as is, so that flag parsing uses the NoOptDefVal.
//
// We have to do this manually as cobra does not support NoOptDefVale with
// positional arguments, see: https://github.com/spf13/cobra/issues/866
func fixFromCloudArgs(args []string) []string {
	// In flag parsing, we check for the last instance of `--from-cloud` in
	// the argument list.
	idx := -1
	for i, arg := range args {
		// We only care about the `--from-cloud` flag. `--from-cloud=value` is
		// already parsed correctly.
		if arg == "--"+fromCloudFlag {
			idx = i
		}
	}
	if idx >= 0 {
		// If it's at the end, there is no value to add. Return the args as is.
		if idx == len(args)-1 {
			return args
		}
		// A cluster ID does not start with a dash, so if the next argument
		// is not a flag, we "fix" the args.
		if !strings.HasPrefix(args[idx+1], "-") {
			newFlag := fmt.Sprintf("--%s=%s", fromCloudFlag, args[idx+1])
			// We remove the value from the args and add it together.
			rest := args[idx+2:]
			args = append(args[:idx], newFlag)
			args = append(args, rest...)
		}
	}
	return args
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
	serverlessNetworking string,
) error {
	if p := yAct.Profile(name); name != "" && p != nil {
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
		err = container.CreateProfile(ctx, fs, c, yAct)
		if err != nil {
			return fmt.Errorf("unable to create profile from rpk container: %v", err)
		}

		// container.CreateProfile writes the yaml file and pushes the
		// new profile to be current. The code is in a separate package
		// because container start automatically creates a profile as
		// well.
		fmt.Printf("Created and switch to profile %q.\n", container.ContainerProfileName)
		fmt.Println("rpk will now talk to your locally running Redpanda container cluster.")
		config.MaybePrintProfileEnvOverrideWarning(container.ContainerProfileName)
		return nil

	case fromCloud != "":
		var err error
		o, err = createCloudProfile(ctx, yAuthVir, cfg, fromCloud, serverlessNetworking)
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
		switch fromProfile {
		case "current":
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
			} else {
				// `rpk profile print -v` outputs the file-backed config as well
				// as the effective config. If we see the annotations from that,
				// we should use only the file-backed portion to create the new profile
				ystr := string(raw)
				annotationPos := strings.Index(ystr, defaultFileAnnotation)
				endPos := strings.Index(ystr, effectiveConfigAnnotation)
				if annotationPos > -1 && endPos > -1 {
					raw = []byte(strings.TrimSpace(ystr[annotationPos+len(defaultFileAnnotation) : endPos]))
				}

				if err := yaml.Unmarshal(raw, &p); err != nil {
					return fmt.Errorf("unable to yaml decode file %q: %v", fromProfile, err)
				}
			}
		}

	case fromRedpanda != "":
		var nodeCfg config.RpkNodeConfig
		switch fromRedpanda {
		case "current":
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
			SR:       nodeCfg.SR,
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

	config.MaybePrintProfileEnvOverrideWarning(p.Name)
	return nil
}

func createCloudProfile(ctx context.Context, yAuthVir *config.RpkCloudAuth, cfg *config.Config, clusterIDOrName string, serverlessNetworking string) (CloudClusterOutputs, error) {
	if yAuthVir == nil {
		return CloudClusterOutputs{}, errors.New("missing current cloud auth, please login with 'rpk cloud login'")
	}

	overrides := cfg.DevOverrides()
	auth0Cl := auth0.NewClient(overrides)
	expired, err := authtoken.ValidateToken(yAuthVir.AuthToken, auth0Cl.Audience(), yAuthVir.ClientID)
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	if expired {
		return CloudClusterOutputs{}, errors.New("current cloud auth has expired, please re-login with 'rpk cloud login'")
	}

	cpCl := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, yAuthVir.AuthToken)
	if clusterIDOrName == "prompt" {
		return PromptCloudClusterProfile(ctx, yAuthVir, cpCl, serverlessNetworking)
	}

	var (
		clusterID       string
		triedNameLookup bool
		forceNameLookup bool
	)
nameLookup:
	_, err = xid.FromString(clusterIDOrName)
	if err != nil || forceNameLookup {
		clusterID, err = clusterNameToID(ctx, cpCl, clusterIDOrName)
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

	sc, err := cpCl.ServerlessClusterForID(ctx, clusterID)
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
		if cluster.State != controlplanev1.Cluster_STATE_READY {
			return CloudClusterOutputs{}, fmt.Errorf("selected cluster %q is not ready for profile creation yet; you may run this command again once the cluster is running", clusterID)
		}
		rg, err := cpCl.ResourceGroupForID(ctx, cluster.GetResourceGroupId())
		if err != nil {
			return CloudClusterOutputs{}, err
		}
		return fromCloudCluster(yAuthVir, rg, cluster), nil
	}
	rg, err := cpCl.ResourceGroupForID(ctx, sc.ResourceGroupId)
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	usePrivate, err := isPrivateNetwork(sc, serverlessNetworking)
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	return fromVirtualCluster(yAuthVir, rg, sc, usePrivate), nil
}

func clusterNameToID(ctx context.Context, cl *publicapi.CloudClientSet, name string) (string, error) {
	_, rgs, scs, cs, err := cl.OrgResourceGroupsClusters(ctx)
	if err != nil {
		return "", fmt.Errorf("unable to request organization, namespace, or cluster details: %w", err)
	}
	candidates := findNamedCluster(name, rgs, scs, cs)

	switch len(candidates) {
	case 0:
		return "", fmt.Errorf("no cluster found with name %q", name)
	case 1:
		for _, nc := range candidates {
			if nc.isServerlessCluster {
				return nc.sCluster.Id, nil
			} else {
				return nc.cluster.Id, nil
			}
		}
		panic("unreachable")
	default:
		ncs := combineClusterNames(rgs, scs, cs)
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
func findNamedCluster(name string, nss []*controlplanev1.ResourceGroup, vcs []*controlplanev1.ServerlessCluster, cs []*controlplanev1.Cluster) map[string]resourceGroupCluster {
	ret := make(map[string]resourceGroupCluster)
	namespaceIDs := make(map[string]*controlplanev1.ResourceGroup, len(nss))
	for _, ns := range nss {
		namespaceIDs[ns.Id] = ns
	}
	for _, vc := range vcs {
		if vc != nil {
			if name != vc.Name && name != fmt.Sprintf("%s/%s", namespaceIDs[vc.ResourceGroupId].Name, vc.Name) {
				continue
			}
			ns := namespaceIDs[vc.ResourceGroupId]
			ret[vc.Name] = resourceGroupCluster{
				resourceGroup:       ns,
				sCluster:            vc,
				isServerlessCluster: true,
			}
		}
	}
	for _, c := range cs {
		if c != nil {
			if name != c.Name && name != fmt.Sprintf("%s/%s", namespaceIDs[c.ResourceGroupId].Name, c.Name) {
				continue
			}
			ns := namespaceIDs[c.ResourceGroupId]
			ret[c.Name] = resourceGroupCluster{
				resourceGroup: ns,
				cluster:       c,
			}
		}
	}
	return ret
}

// fromCloudCluster returns an rpk profile from a cloud cluster, as well
// as if the cluster requires mtls or sasl.
func fromCloudCluster(yAuth *config.RpkCloudAuth, rg *controlplanev1.ResourceGroup, c *controlplanev1.Cluster) CloudClusterOutputs {
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
	if c.SchemaRegistry != nil {
		p.SR.Addresses = []string{c.SchemaRegistry.Url}
		p.SR.TLS = new(config.TLS)
		if mtls := c.SchemaRegistry.Mtls; !isMTLS && mtls != nil {
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

func fromVirtualCluster(yAuth *config.RpkCloudAuth, rg *controlplanev1.ResourceGroup, sc *controlplanev1.ServerlessCluster, usePrivate bool) CloudClusterOutputs {
	// Determine which URLs to use based on usePrivate flag
	var (
		seedBrokers []string
		consoleURL  string
		schemaURL   string
	)

	if usePrivate {
		if sc.KafkaApi != nil {
			seedBrokers = sc.KafkaApi.PrivateSeedBrokers
		}
		consoleURL = sc.ConsolePrivateUrl
		if sc.SchemaRegistry != nil {
			schemaURL = sc.SchemaRegistry.PrivateUrl
		}
	} else {
		if sc.KafkaApi != nil {
			seedBrokers = sc.KafkaApi.SeedBrokers
		}
		consoleURL = sc.ConsoleUrl
		if sc.SchemaRegistry != nil {
			schemaURL = sc.SchemaRegistry.Url
		}
	}

	p := config.RpkProfile{
		Name:      sc.Name,
		FromCloud: true,
		KafkaAPI: config.RpkKafkaAPI{
			Brokers: seedBrokers,
			TLS:     new(config.TLS),
			SASL: &config.SASL{
				Mechanism: adminapi.CloudOIDC,
			},
		},
		AdminAPI: config.RpkAdminAPI{
			Addresses: []string{consoleURL},
			TLS:       new(config.TLS),
		},
		SR: config.RpkSchemaRegistryAPI{
			Addresses: []string{schemaURL},
			TLS:       new(config.TLS),
		},
		CloudCluster: config.RpkCloudCluster{
			ResourceGroup: rg.Name,
			ClusterID:     sc.Id,
			ClusterName:   sc.Name,
			AuthOrgID:     yAuth.OrgID,
			AuthKind:      yAuth.Kind,
			ClusterType:   config.ServerlessClusterType, // Virtual clusters do not include a type in the response yet.
		},
	}

	return CloudClusterOutputs{
		Profile:           p,
		ResourceGroupName: rg.Name,
		ClusterName:       sc.Name,
		ClusterID:         sc.Id,
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
func PromptCloudClusterProfile(ctx context.Context, yAuth *config.RpkCloudAuth, cl *publicapi.CloudClientSet, serverlessNetworking string) (CloudClusterOutputs, error) {
	org, rgs, scs, cs, err := cl.OrgResourceGroupsClusters(ctx)
	if err != nil {
		return CloudClusterOutputs{}, err
	}
	if len(cs) == 0 && len(scs) == 0 {
		return CloudClusterOutputs{}, ErrNoCloudClusters
	}

	// Always prompt, even if there is only one option.
	ncs := combineClusterNames(rgs, scs, cs)
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
	if selected.c != nil {
		// Validate serverless networking flags aren't used with non-serverless clusters
		if serverlessNetworking != "" {
			return CloudClusterOutputs{}, fmt.Errorf("--serverless-network flag can only be used with serverless clusters")
		}
		// We have a selected cluster, but the list response does not return
		// all the information we need.
		c, err := cl.ClusterForID(ctx, selected.c.Id)
		if err != nil {
			return CloudClusterOutputs{}, err
		}
		rg := findResourceGroupByID(rgs, c.GetResourceGroupId())
		if rg == nil {
			return CloudClusterOutputs{}, fmt.Errorf("unable to find resource group %q", c.GetResourceGroupId())
		}
		o = fromCloudCluster(yAuth, rg, c)
	} else {
		// Fetch full cluster details to get NetworkingConfig
		sc, err := cl.ServerlessClusterForID(ctx, selected.sc.Id)
		if err != nil {
			return CloudClusterOutputs{}, err
		}
		rg := findResourceGroupByID(rgs, sc.GetResourceGroupId())
		if rg == nil {
			return CloudClusterOutputs{}, fmt.Errorf("unable to find resource group %q", sc.GetResourceGroupId())
		}
		usePrivate, err := isPrivateNetwork(sc, serverlessNetworking)
		if err != nil {
			return CloudClusterOutputs{}, err
		}
		o = fromVirtualCluster(yAuth, rg, sc, usePrivate)
	}
	o.Profile.Description = fmt.Sprintf("%s %q", org.Name, selected.name)
	return o, nil
}

func findResourceGroupByID(rgs []*controlplanev1.ResourceGroup, id string) *controlplanev1.ResourceGroup {
	for _, r := range rgs {
		if r.Id == id {
			return r
		}
	}
	return nil
}

// isPrivateNetwork determines whether to use private networking based on
// the cluster's NetworkingConfig. It returns true if private networking should be used.
// It only prompts if both private and public are enabled and no override is specified.
// The override parameter can be: "" (no override), "private", or "public".
func isPrivateNetwork(sc *controlplanev1.ServerlessCluster, override string) (bool, error) {
	if sc == nil || sc.NetworkingConfig == nil {
		// No cluster or no networking config means use default public
		if override == "private" {
			return false, fmt.Errorf("cluster does not support private networking")
		}
		return false, nil
	}

	privateEnabled := sc.NetworkingConfig.Private == controlplanev1.ServerlessNetworkingConfig_STATE_ENABLED
	publicEnabled := sc.NetworkingConfig.Public == controlplanev1.ServerlessNetworkingConfig_STATE_ENABLED ||
		sc.NetworkingConfig.Public == controlplanev1.ServerlessNetworkingConfig_STATE_UNSPECIFIED

	// If an override is specified, validate and use it
	if override != "" {
		switch override {
		case "private":
			if !privateEnabled {
				return false, fmt.Errorf("cluster does not have private networking enabled")
			}
			return true, nil
		case "public":
			if !publicEnabled {
				return false, fmt.Errorf("cluster does not have public networking enabled")
			}
			return false, nil
		default:
			return false, fmt.Errorf("invalid networking override: %q", override)
		}
	}

	// If both are enabled, prompt the user
	if privateEnabled && publicEnabled {
		options := []string{"Public", "Private"}
		idx, err := out.PickIndex(options, "This cluster supports both public and private networking. Which would you like to use?")
		if err != nil {
			return false, err
		}
		return idx == 1, nil // 1 = Private
	}

	// If only private is enabled, use private
	if privateEnabled {
		return true, nil
	}

	// Otherwise use public (default)
	return false, nil
}

// nameAndCluster describes a cluster name in the form of
// <namespace>/<cluster-name> and the cluster type (virtual, normal).
type nameAndCluster struct {
	name string
	c    *controlplanev1.Cluster
	sc   *controlplanev1.ServerlessCluster
}

func (nc *nameAndCluster) clusterID() string {
	if nc.c != nil {
		return nc.c.Id
	}
	return nc.sc.Id
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
func combineClusterNames(rgs []*controlplanev1.ResourceGroup, scs []*controlplanev1.ServerlessCluster, cs []*controlplanev1.Cluster) namesAndClusters {
	rgIDToName := make(map[string]string, len(rgs))
	for _, rg := range rgs {
		if rg != nil {
			rgIDToName[rg.Id] = rg.Name
		}
	}

	// First we display the Serverless Clusters
	var sNameAndCs []nameAndCluster
	for _, sc := range scs {
		if sc != nil {
			sc := sc
			if sc.State != controlplanev1.ServerlessCluster_STATE_READY {
				continue
			}
			sNameAndCs = append(sNameAndCs, nameAndCluster{
				name: fmt.Sprintf("%s/%s", rgIDToName[sc.ResourceGroupId], sc.Name),
				sc:   sc,
			})
		}
	}
	sort.Slice(sNameAndCs, func(i, j int) bool {
		return sNameAndCs[i].name < sNameAndCs[j].name
	})

	// Then we append the cluster names
	var nameAndCs []nameAndCluster
	for _, c := range cs {
		if c.State != controlplanev1.Cluster_STATE_READY {
			continue
		}
		nameAndCs = append(nameAndCs, nameAndCluster{
			name: fmt.Sprintf("%s/%s", rgIDToName[c.ResourceGroupId], c.Name),
			c:    c,
		})
	}
	sort.Slice(nameAndCs, func(i, j int) bool {
		return nameAndCs[i].name < nameAndCs[j].name
	})

	return append(sNameAndCs, nameAndCs...)
}

// resourceGroupCluster ties a cluster or serverless cluster to its resource
// group.
type resourceGroupCluster struct {
	resourceGroup       *controlplanev1.ResourceGroup
	cluster             *controlplanev1.Cluster
	sCluster            *controlplanev1.ServerlessCluster
	isServerlessCluster bool
}
