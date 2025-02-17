package clienttest

import (
	"context"
	"fmt"
	"net"
	"os"
	"slices"
	"time"

	"github.com/fatih/color"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"golang.org/x/term"
)

type resultLevel int

const (
	levelPass resultLevel = iota
	levelWarn
	levelFail
)

var resultString = map[resultLevel]string{
	levelPass: "Pass",
	levelWarn: "Warn",
	levelFail: "Fail",
}

var colourResultString = map[resultLevel]string{
	levelPass: color.GreenString("Pass"),
	levelWarn: color.YellowString("Warn"),
	levelFail: color.RedString("Fail"),
}

func (rl resultLevel) String() string {
	if term.IsTerminal(int(os.Stdout.Fd())) {
		return colourResultString[rl]
	} else {
		return resultString[rl]
	}
}

type result struct {
	level   resultLevel
	message string
}

type resultGroup struct {
	name    string
	results []result
}

type check struct {
	name string
	f    func() ([]result, error)
}

type checkParams struct {
	profile *config.RpkProfile
	fs      afero.Fs
	adm     *kadm.Client
}

func checkResolution(cp checkParams) check {
	return check{"DNS Resolution", func() ([]result, error) {
		results := []result{}
		sources := map[string][]string{
			"Kafka API":       cp.profile.KafkaAPI.Brokers,
			"Schema Registry": cp.profile.SR.Addresses,
			"Admin API":       cp.profile.AdminAPI.Addresses,
		}
		hosts := map[string][]string{}
		for source, addrs := range sources {
			for _, addr := range addrs {
				host, _, _ := net.SplitHostPort(addr)
				if !slices.Contains(hosts[host], source) {
					hosts[host] = append(hosts[host], source)
				}
			}
		}

		for host, source := range hosts {
			resolved, err := net.LookupHost(host)
			if err == nil {
				results = append(results, result{levelPass, fmt.Sprintf("%s (from %q) resolves to: %q", host, source, resolved)})
			} else {
				results = append(results, result{levelFail, fmt.Sprintf("error resolving %s (from %q): %q", host, source, err)})
			}
		}
		return results, nil
	}}
}

func checkConnectivity(cp checkParams) check {
	return check{"Connectivity", func() ([]result, error) {
		results := []result{}
		sources := map[string][]string{
			"Kafka API":       cp.profile.KafkaAPI.Brokers,
			"Schema Registry": cp.profile.SR.Addresses,
			"Admin API":       cp.profile.AdminAPI.Addresses,
		}
		testAddrs := map[string][]string{}
		for source, addrs := range sources {
			for _, addr := range addrs {
				if !slices.Contains(testAddrs[addr], source) {
					testAddrs[addr] = append(testAddrs[addr], source)
				}
			}
		}
		for addr, source := range testAddrs {
			host, port, _ := net.SplitHostPort(addr)
			resolved, _ := net.LookupHost(host)
			for _, ra := range resolved {
				_, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", ra, port), time.Second*10)
				if err == nil {
					results = append(results, result{levelPass, fmt.Sprintf("Successfully connected to %s:%s (from %q)", ra, port, source)})
				} else {
					results = append(results, result{levelFail, fmt.Sprintf("Failed to connect to %s:%s (from %q) - %q", ra, port, source, err)})
				}
			}
		}

		return results, nil
	}}
}

func checkKafkaAPI(cp checkParams) check {
	return check{"Kafka API", func() ([]result, error) {
		results := []result{}
		meta, err := cp.adm.BrokerMetadata(context.Background())
		if err != nil {
			results = append(results, result{levelFail, fmt.Sprintf("Failed to connect to Kafka API - %q", err)})
		} else {
			results = append(results, result{levelPass, fmt.Sprintf("Connected to Kafka API - Cluster: %s", meta.Cluster)})
		}
		return results, nil
	}}
}

func checkSchemaRegistry(cp checkParams) check {
	return check{"Schema Registry", func() ([]result, error) {
		cl, err := schemaregistry.NewClient(cp.fs, cp.profile)
		if err != nil {
			return []result{{levelFail, fmt.Sprintf("Failed to initialize schema registry client: %q", err)}}, nil
		}
		subjects, err := cl.Subjects(context.Background())
		if err != nil {
			return []result{{levelFail, fmt.Sprintf("Failed to list subjects: %q", err)}}, nil
		}
		return []result{{levelPass, fmt.Sprintf("Successfully listed subjects (%d found)", len(subjects))}}, nil
	}}
}

func checkAdminApi(cp checkParams) check {
	return check{"Admin API", func() ([]result, error) {
		client, err := adminapi.NewClient(context.Background(), cp.fs, cp.profile)
		if err != nil {
			return []result{{levelFail, fmt.Sprintf("Failed to initialize Admin api client: %q", err)}}, nil
		}

		uuid, err := client.ClusterUUID(context.Background())
		if err != nil {
			return []result{{levelFail, fmt.Sprintf("Failed to access Admin API: %q", err)}}, nil
		}
		return []result{{levelPass, fmt.Sprintf("Successfully got cluster UUID from Admin API - %s", uuid.UUID)}}, nil
	}}
}

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {

	cmd := &cobra.Command{
		Use:   "client-test",
		Short: "Test client connectivity",
		Long:  "Test client connectivity from this machine to a defined Redpanda/Kafka cluster",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize admin kafka client: %v", err)
			cp := checkParams{
				profile: p,
				fs:      fs,
				adm:     adm,
			}
			checks := []check{
				checkResolution(cp),
				checkConnectivity(cp),
				checkKafkaAPI(cp),
				checkSchemaRegistry(cp),
				checkAdminApi(cp),
			}
			resultGroups := []resultGroup{}
			for _, c := range checks {
				fmt.Printf("Running check: %s\n", c.name)
				r, err := c.f()
				if err != nil {
					fmt.Printf("Error running check %s - %q", c.name, err)
					continue
				}
				resultGroups = append(resultGroups, resultGroup{c.name, r})

			}
			fmt.Printf("Checks complete - %d checks run\n\n", len(resultGroups))
			for _, rg := range resultGroups {
				fmt.Printf("%s\n", rg.name)
				for _, r := range rg.results {
					fmt.Printf("\t[%s] - %s\n", r.level.String(), r.message)
				}
			}
		},
	}

	p.InstallKafkaFlags(cmd)
	p.InstallAdminFlags(cmd)

	return cmd
}
