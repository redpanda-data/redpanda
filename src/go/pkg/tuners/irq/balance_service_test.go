package irq

import (
	"testing"
	"vectorized/pkg/os"
	"vectorized/pkg/utils"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

type procMock struct {
	os.Proc
	run       func(command string, args ...string) ([]string, error)
	isRunning func(processName string) bool
}

func (procMock *procMock) RunWithSystemLdPath(
	command string, args ...string,
) ([]string, error) {
	return procMock.run(command, args...)
}

func (procMock *procMock) IsRunning(processName string) bool {
	return procMock.isRunning(processName)
}

func Test_BalanceService_BanIRQsAndRestart(t *testing.T) {

	running := func(processName string) bool {
		return true
	}

	type fields struct {
		proc os.Proc
		fs   afero.Fs
	}
	type args struct {
		bannedIRQs []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		before func(fields)
		assert func(fields, error)
	}{
		{
			name: "Shall update the config and then restart IRQ " +
				"balance service with config  in /etc/sysconfig/irqbalance & systemd",
			fields: fields{
				proc: &procMock{
					isRunning: running,
					run: func(command string, args ...string) (strings []string, e error) {
						assert.Equal(t, "systemctl", command)
						assert.Equal(t, []string{"try-restart", "irqbalance"}, args)
						return nil, nil
					},
				},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				bannedIRQs: []int{5, 12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					[]string{"ONE_SHOT=true", "#IRQBALANCE_BANNED_CPUS="},
					"/etc/sysconfig/irqbalance")
			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(fields.fs, "/etc/sysconfig/irqbalance.rpk.orig")
				assert.Equal(t, 2, len(backupFileContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fields.fs, "/etc/sysconfig/irqbalance")
				assert.Equal(t, 3, len(fileContent))
				assert.Equal(t, "IRQBALANCE_ARGS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
		{
			name: "Shall add  IRQs to banned list leaving those that were already banned intact",
			fields: fields{
				proc: &procMock{
					isRunning: running,
					run: func(command string, args ...string) (strings []string, e error) {
						assert.Equal(t, "systemctl", command)
						assert.Equal(t, []string{"try-restart", "irqbalance"}, args)
						return nil, nil
					},
				},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				bannedIRQs: []int{12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					[]string{"ONE_SHOT=true",
						"#IRQBALANCE_BANNED_CPUS=",
						"IRQBALANCE_ARGS=\" --banirq=5\""},
					"/etc/sysconfig/irqbalance")
			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(fields.fs, "/etc/sysconfig/irqbalance.rpk.orig")
				assert.Equal(t, 3, len(backupFileContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fields.fs, "/etc/sysconfig/irqbalance")
				assert.Equal(t, 3, len(fileContent))
				assert.Equal(t, "IRQBALANCE_ARGS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
		{
			name: "Shall prevent duplicates in banned IRQs arguments",
			fields: fields{
				proc: &procMock{
					isRunning: running,
					run: func(command string, args ...string) (strings []string, e error) {
						assert.Equal(t, "systemctl", command)
						assert.Equal(t, []string{"try-restart", "irqbalance"}, args)
						return nil, nil
					},
				},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				bannedIRQs: []int{5, 12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					[]string{"ONE_SHOT=true",
						"#IRQBALANCE_BANNED_CPUS=",
						// IRQ 5 is already banned
						"IRQBALANCE_ARGS=\" --banirq=5\""},
					"/etc/sysconfig/irqbalance")
			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(fields.fs, "/etc/sysconfig/irqbalance.rpk.orig")
				assert.Equal(t, 3, len(backupFileContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fields.fs, "/etc/sysconfig/irqbalance")
				assert.Equal(t, 3, len(fileContent))
				assert.Equal(t, "IRQBALANCE_ARGS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
		{
			name: "Shall update the config and then restart IRQ " +
				"balance service with config  in /etc/conf.d/irqbalance & systemd",
			fields: fields{
				proc: &procMock{
					isRunning: running,
					run: func(command string, args ...string) (strings []string, e error) {
						assert.Equal(t, "systemctl", command)
						assert.Equal(t, []string{"try-restart", "irqbalance"}, args)
						return nil, nil
					},
				},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				bannedIRQs: []int{5, 12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					[]string{"ONE_SHOT=true", "#IRQBALANCE_BANNED_CPUS="},
					"/etc/conf.d/irqbalance")
				_ = utils.WriteFileLines(fields.fs,
					[]string{"systemd"},
					"/proc/1/comm")

			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(fields.fs, "/etc/conf.d/irqbalance.rpk.orig")
				assert.Equal(t, 2, len(backupFileContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fields.fs, "/etc/conf.d/irqbalance")
				assert.Equal(t, 3, len(fileContent))
				assert.Equal(t, "IRQBALANCE_OPTS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
		{
			name: "Shall update the config and then restart IRQ " +
				"balance service with config  in /etc/conf.d/irqbalance & init daemon",
			fields: fields{
				proc: &procMock{
					isRunning: running,
					run: func(command string, args ...string) (strings []string, e error) {
						assert.Equal(t, "/etc/init.d/irqbalance", command)
						assert.Equal(t, []string{"restart"}, args)
						return nil, nil
					},
				},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				bannedIRQs: []int{5, 12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					[]string{"ONE_SHOT=true", "#IRQBALANCE_BANNED_CPUS="},
					"/etc/conf.d/irqbalance")
				_ = utils.WriteFileLines(fields.fs,
					[]string{"init"},
					"/proc/1/comm")

			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(fields.fs, "/etc/conf.d/irqbalance.rpk.orig")
				assert.Equal(t, 2, len(backupFileContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fields.fs, "/etc/conf.d/irqbalance")
				assert.Equal(t, 3, len(fileContent))
				assert.Equal(t, "IRQBALANCE_OPTS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before(tt.fields)
			balanceService := NewBalanceService(
				tt.fields.fs,
				tt.fields.proc,
			)
			tt.assert(tt.fields, balanceService.BanIRQsAndRestart(tt.args.bannedIRQs))

		})
	}
}
