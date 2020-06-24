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
	statuses, err := c.conn.ListUnitsByNames([]string{name})
	if err != nil {
		return LoadStateUnknown, ActiveStateUnknown, err
	}
	if len(statuses) == 0 {
		return LoadStateUnknown, ActiveStateUnknown, nil
	}
	status := statuses[0]
	return toLoadState(status.LoadState), toActiveState(status.ActiveState), nil
}

func (c *dbusClient) LoadUnit(fs afero.Fs, body, name string) error {
	_, err := utils.WriteBytes(fs, []byte(body), UnitPath(name))
	if err != nil {
		return err
	}
	return c.conn.Reload()
}
