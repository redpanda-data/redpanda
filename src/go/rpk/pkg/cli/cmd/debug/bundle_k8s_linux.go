// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package debug

import (
	"archive/zip"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kgo"
)

func executeK8SBundle(
	ctx context.Context,
	fs afero.Fs,
	conf *config.Config,
	cl *kgo.Client,
	admin *admin.AdminAPI,
	timeout time.Duration,
	controllerLogLimitBytes int,
	path string,
) error {
	fmt.Println("Creating bundle file...")
	mode := os.FileMode(0o755)
	f, err := fs.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY,
		mode,
	)
	if err != nil {
		return fmt.Errorf("unable to create bundle file: %v", err)
	}
	defer f.Close()

	grp := multierror.Group{}

	w := zip.NewWriter(f)
	defer w.Close()

	ps := &stepParams{
		fs:      fs,
		w:       w,
		timeout: timeout,
	}

	steps := []step{
		saveKafkaMetadata(ctx, ps, cl),
		saveDataDirStructure(ps, conf),
		saveConfig(ps, conf),
		saveCPUInfo(ps),
		saveInterrupts(ps),
		saveResourceUsageData(ps, conf),
		saveNTPDrift(ps),
		savePrometheusMetrics(ctx, ps, admin),
		saveDiskUsage(ctx, ps, conf),
		saveControllerLogDir(fs, ps, conf, controllerLogLimitBytes),
	}
	for _, s := range steps {
		grp.Go(s)
	}

	errs := grp.Wait()
	if errs != nil {
		err := writeFileToZip(ps, "errors.txt", []byte(errs.Error()))
		if err != nil {
			errs = multierror.Append(errs, err)
		}
		fmt.Println(errs.Error())
	}

	fmt.Printf("Debug bundle saved to %q\n", f.Name())
	return nil
}
