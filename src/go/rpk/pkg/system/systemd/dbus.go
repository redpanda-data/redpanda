// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package systemd

import (
	"github.com/coreos/go-systemd/v22/dbus"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
)

type dbusClient struct {
	conn *dbus.Conn
}

func NewDbusClient() (Client, error) {
	conn, err := dbus.New()
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
	_, err := c.conn.StartUnit(name, "replace", nil)
	if err != nil {

		logrus.Error("ERROR Starting unit ", err)
	}
	return err
}

func (c *dbusClient) UnitState(name string) (LoadState, ActiveState, error) {
	loadState, err := c.conn.GetUnitProperty(name, "LoadState")
	if err != nil {
		return LoadStateUnknown, ActiveStateUnknown, nil
	}
	activeState, err := c.conn.GetUnitProperty(name, "ActiveState")
	if err != nil {
		return toLoadState(loadState.Value.String()),
			ActiveStateUnknown,
			nil
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
	return c.conn.Reload()
}
