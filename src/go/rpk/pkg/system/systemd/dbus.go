// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package systemd

import (
	"context"

	"github.com/coreos/go-systemd/v22/dbus"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
)

type dbusClient struct {
	conn *dbus.Conn
}

func NewDbusClient() (Client, error) {
	conn, err := dbus.NewWithContext(context.Background())
	if err != nil {
		return nil, err
	}
	return &dbusClient{conn: conn}, nil
}

func (c *dbusClient) Shutdown() error {
	c.conn.Close()
	return nil
}

func (c *dbusClient) StartUnit(name string) error {
	_, err := c.conn.StartUnitContext(context.Background(), name, "replace", nil)
	return err
}

func (c *dbusClient) UnitState(name string) (LoadState, ActiveState, error) {
	loadState, err := c.conn.GetUnitPropertyContext(context.Background(), name, "LoadState")
	if err != nil {
		return LoadStateUnknown, ActiveStateUnknown, err
	}
	activeState, err := c.conn.GetUnitPropertyContext(context.Background(), name, "ActiveState")
	if err != nil {
		return toLoadState(loadState.Value.String()),
			ActiveStateUnknown,
			err
	}

	return toLoadState(loadState.Value.String()),
		toActiveState(activeState.Value.String()),
		nil
}

func (c *dbusClient) LoadUnit(fs afero.Fs, body, name string) error {
	_, err := utils.WriteBytes(fs, []byte(body), UnitPath(name))
	if err != nil {
		return err
	}
	return c.conn.ReloadContext(context.Background())
}
