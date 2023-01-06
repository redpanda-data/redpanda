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

	"github.com/hashicorp/go-multierror"
)

func executeK8SBundle(ctx context.Context, bp bundleParams) error {
	fmt.Println("Creating bundle file...")
	mode := os.FileMode(0o755)
	f, err := bp.fs.OpenFile(
		bp.path,
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
		fs:      bp.fs,
		w:       w,
		timeout: bp.timeout,
	}

	steps := []step{
		saveKafkaMetadata(ctx, ps, bp.cl),
		saveDataDirStructure(ps, bp.cfg),
		saveConfig(ps, bp.cfg),
		saveCPUInfo(ps),
		saveInterrupts(ps),
		saveResourceUsageData(ps, bp.cfg),
		saveNTPDrift(ps),
		savePrometheusMetrics(ctx, ps, bp.admin),
		saveDiskUsage(ctx, ps, bp.cfg),
		saveControllerLogDir(bp.fs, ps, bp.cfg, bp.controllerLogLimitBytes),
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
