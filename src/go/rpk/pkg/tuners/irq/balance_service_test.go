package irq

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"
	"time"
	"vectorized/pkg/os"
	"vectorized/pkg/tuners/executors"
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
	_ time.Duration, command string, args ...string,
) ([]string, error) {
	return procMock.run(command, args...)
}

func (procMock *procMock) IsRunning(_ time.Duration, processName string) bool {
	return procMock.isRunning(processName)
}

type balanceServiceMock struct {
	BalanceService
	getBannedIRQs func() ([]int, error)
	isRunning     bool
}

func (m *balanceServiceMock) BanIRQsAndRestart(bannedIRQs []int) error {
	panic("not implemented")
}

func (m *balanceServiceMock) GetBannedIRQs() ([]int, error) {
	return m.getBannedIRQs()
}

func (m *balanceServiceMock) IsRunning() bool {
	return m.isRunning
}

func Test_BalanceService_BanIRQsAndRestart(t *testing.T) {

	running := func(processName string) bool {
		return true
	}

	type fields struct {
		proc       os.Proc
		fs         afero.Fs
		configFile []string
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
				configFile: []string{"ONE_SHOT=true", "#IRQBALANCE_BANNED_CPUS="},
				fs:         afero.NewMemMapFs(),
			},
			args: args{
				bannedIRQs: []int{5, 12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					fields.configFile,
					"/etc/sysconfig/irqbalance")
			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				md5 := calcMd5(fields.configFile)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(
					fields.fs, fmt.Sprintf("/etc/sysconfig/irqbalance.vectorized.%s.bk", md5))
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
				configFile: []string{"ONE_SHOT=true",
					"#IRQBALANCE_BANNED_CPUS=",
					"IRQBALANCE_ARGS=\" --banirq=5\""},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				bannedIRQs: []int{12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					fields.configFile,
					"/etc/sysconfig/irqbalance")
			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				md5 := calcMd5(fields.configFile)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(
					fields.fs, fmt.Sprintf("/etc/sysconfig/irqbalance.vectorized.%s.bk", md5))
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
				configFile: []string{"ONE_SHOT=true",
					"#IRQBALANCE_BANNED_CPUS=",
					// IRQ 5 is already banned
					"IRQBALANCE_ARGS=\" --banirq=5\""},
			},
			args: args{
				bannedIRQs: []int{5, 12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					fields.configFile,
					"/etc/sysconfig/irqbalance")
			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				md5 := calcMd5(fields.configFile)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(
					fields.fs, fmt.Sprintf("/etc/sysconfig/irqbalance.vectorized.%s.bk", md5))
				assert.Equal(t, 3, len(backupFileContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fields.fs, "/etc/sysconfig/irqbalance")
				assert.Equal(t, 3, len(fileContent))
				assert.Equal(t, "IRQBALANCE_ARGS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
		{
			name: "Shall update the config and then restart IRQ " +
				"balance service with config in /etc/conf.d/irqbalance & systemd",
			fields: fields{
				proc: &procMock{
					isRunning: running,
					run: func(command string, args ...string) (strings []string, e error) {
						assert.Equal(t, "systemctl", command)
						assert.Equal(t, []string{"try-restart", "irqbalance"}, args)
						return nil, nil
					},
				},
				configFile: []string{"ONE_SHOT=true", "#IRQBALANCE_BANNED_CPUS="},
				fs:         afero.NewMemMapFs(),
			},
			args: args{
				bannedIRQs: []int{5, 12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					fields.configFile,
					"/etc/conf.d/irqbalance")
				_ = utils.WriteFileLines(fields.fs,
					[]string{"systemd"},
					"/proc/1/comm")

			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				md5 := calcMd5(fields.configFile)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(fields.fs,
					fmt.Sprintf("/etc/conf.d/irqbalance.vectorized.%s.bk", md5))
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
				configFile: []string{"ONE_SHOT=true", "#IRQBALANCE_BANNED_CPUS="},
				fs:         afero.NewMemMapFs(),
			},
			args: args{
				bannedIRQs: []int{5, 12, 15},
			},
			before: func(fields fields) {
				_ = utils.WriteFileLines(fields.fs,
					fields.configFile,
					"/etc/conf.d/irqbalance")
				_ = utils.WriteFileLines(fields.fs,
					[]string{"init"},
					"/proc/1/comm")

			},
			assert: func(fields fields, err error) {
				assert.Nil(t, err)
				md5 := calcMd5(fields.configFile)
				// Check if backup is created
				backupFileContent, err := utils.ReadFileLines(
					fields.fs, fmt.Sprintf("/etc/conf.d/irqbalance.vectorized.%s.bk", md5))
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
				executors.NewDirectExecutor(),
				time.Duration(10)*time.Second,
			)
			tt.assert(tt.fields, balanceService.BanIRQsAndRestart(tt.args.bannedIRQs))

		})
	}
}

func Test_balanceService_GetBannedIRQs(t *testing.T) {
	type fields struct {
		fs     afero.Fs
		proc   os.Proc
		before func(afero.Fs)
	}
	tests := []struct {
		name    string
		fields  fields
		want    []int
		wantErr bool
	}{
		{
			name: "Shall return all banned irq",
			fields: fields{
				proc: &procMock{},
				fs:   afero.NewMemMapFs(),
				before: func(fs afero.Fs) {
					_ = utils.WriteFileLines(fs,
						[]string{"ONE_SHOT=true",
							"#IRQBALANCE_BANNED_CPUS=",
							"IRQBALANCE_ARGS=\"--other --banirq=123 --else=12" +
								"--banirq=34 --banirq=48 --banirq=16\""},
						"/etc/sysconfig/irqbalance")
				},
			},
			want:    []int{123, 34, 48, 16},
			wantErr: false,
		},
		{
			name: "Shall return empty list as there are none banned IRQs",
			fields: fields{
				proc: &procMock{},
				fs:   afero.NewMemMapFs(),
				before: func(fs afero.Fs) {
					_ = utils.WriteFileLines(fs,
						[]string{"ONE_SHOT=true",
							"#IRQBALANCE_BANNED_CPUS=",
							"IRQBALANCE_ARGS=\"--other --else --third\""},
						"/etc/sysconfig/irqbalance")
				},
			},
			wantErr: false,
		},
		{
			name: "Shall return empty list as there are no custom options line",
			fields: fields{
				proc: &procMock{},
				fs:   afero.NewMemMapFs(),
				before: func(fs afero.Fs) {
					_ = utils.WriteFileLines(fs,
						[]string{"ONE_SHOT=true",
							"#IRQBALANCE_BANNED_CPUS="},
						"/etc/sysconfig/irqbalance")
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			balanceService := &balanceService{
				fs:   tt.fields.fs,
				proc: tt.fields.proc,
			}
			tt.fields.before(tt.fields.fs)
			got, err := balanceService.GetBannedIRQs()
			if (err != nil) != tt.wantErr {
				t.Errorf("balanceService.GetBannedIRQs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("balanceService.GetBannedIRQs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAreIRQsStaticallyAssigned(t *testing.T) {
	type args struct {
		irqs           []int
		balanceService BalanceService
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "Shall return true as all requested IRQs are banned",
			args: args{
				balanceService: &balanceServiceMock{
					getBannedIRQs: func() ([]int, error) {
						return []int{12, 56, 87, 34, 46}, nil
					},
					isRunning: true,
				},
				irqs: []int{12, 34},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Shall return false when some of the requested IRQs are not banned",
			args: args{
				balanceService: &balanceServiceMock{
					getBannedIRQs: func() ([]int, error) {
						return []int{12, 56, 87, 34, 46}, nil
					},
					isRunning: true,
				},
				irqs: []int{12, 134},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "Shall always return true when irqbalance is not running",
			args: args{
				balanceService: &balanceServiceMock{
					isRunning: false,
				},
				irqs: []int{12, 134},
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AreIRQsStaticallyAssigned(tt.args.irqs, tt.args.balanceService)
			if (err != nil) != tt.wantErr {
				t.Errorf("AreIRQsStaticallyAssigned() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AreIRQsStaticallyAssigned() = %v, want %v", got, tt.want)
			}
		})
	}
}

func calcMd5(lines []string) string {
	var data []byte
	for _, line := range lines {
		for _, dataByte := range []byte(line + "\n") {
			data = append(data, dataByte)
		}
	}
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:16])
}
