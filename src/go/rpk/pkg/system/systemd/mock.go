package systemd

import "github.com/spf13/afero"

type mockClient struct {
	shutdown  func() error
	startUnit func(string) error
	unitState func(string) (LoadState, ActiveState, error)
	loadUnit  func(afero.Fs, string, string) error
}

func NewMockClient(
	shutdown func() error,
	startUnit func(string) error,
	unitState func(string) (LoadState, ActiveState, error),
	loadUnit func(afero.Fs, string, string) error,
) Client {
	return &mockClient{
		shutdown:  shutdown,
		startUnit: startUnit,
		unitState: unitState,
		loadUnit:  loadUnit,
	}
}

func (c *mockClient) Shutdown() error {
	return c.shutdown()
}
func (c *mockClient) StartUnit(name string) error {
	return c.startUnit(name)
}

func (c *mockClient) UnitState(name string) (LoadState, ActiveState, error) {
	return c.unitState(name)
}

func (c *mockClient) LoadUnit(fs afero.Fs, body, name string) error {
	return c.loadUnit(fs, body, name)
}
