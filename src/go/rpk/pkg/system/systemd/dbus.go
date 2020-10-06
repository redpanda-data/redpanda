package systemd

import (
	"vectorized/pkg/utils"

	"github.com/coreos/go-systemd/v22/dbus"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
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
