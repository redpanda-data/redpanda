package system

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
	"vectorized/pkg/os"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/utils"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

type fields struct {
	commands os.Commands
	fs       afero.Fs
	grubCfg  []string
}

func TestGrubAddCommandLineOptions(t *testing.T) {

	type args struct {
		opt []string
	}
	tests := []struct {
		name    string
		before  func(fields)
		fields  fields
		args    args
		wantErr bool
		check   func(fields)
	}{
		{
			name:   "Shall add new value only flag to GRUB cfg",
			before: persistGrubConfig,
			fields: fields{
				grubCfg: []string{
					"GRUB_TIMEOUT=5",
					"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
					"GRUB_TERMINAL_OUTPUT=\"console\"",
					"GRUB_DEFAULT=saved",
					"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap rd.lvm.lv=fedora/root\"",
					"GRUB_DISABLE_SUBMENU=true",
				},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				opt: []string{"noht"},
			},
			wantErr: false,
			check: func(fields fields) {
				md5 := calcMd5(fields.grubCfg)
				backupName := fmt.Sprintf("/etc/default/grub.vectorized.%s.bk", md5)
				backup, err := utils.ReadFileLines(fields.fs, backupName)
				assert.Nil(t, err)
				assert.Equal(t, fields.grubCfg, backup)
				lines, _ := utils.ReadFileLines(fields.fs, "/etc/default/grub")
				assert.Len(t, lines, 6)
				opts := getGrubCmdLineOptsLine(lines)
				assert.Len(t, opts, 3)
				assert.Contains(t, opts, "noht")
			},
		},
		{
			name:   "Shall add new key/value pair flag to GRUB cfg",
			before: persistGrubConfig,
			fields: fields{
				grubCfg: []string{
					"GRUB_TIMEOUT=5",
					"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
					"GRUB_TERMINAL_OUTPUT=\"console\"",
					"GRUB_DEFAULT=saved",
					"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap rd.lvm.lv=fedora/root\"",
					"GRUB_DISABLE_SUBMENU=true",
				},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				opt: []string{"some_opt=2"},
			},
			wantErr: false,
			check: func(fields fields) {
				md5 := calcMd5(fields.grubCfg)
				backupName := fmt.Sprintf("/etc/default/grub.vectorized.%s.bk", md5)
				backup, err := utils.ReadFileLines(fields.fs, backupName)
				assert.Nil(t, err)
				assert.Equal(t, fields.grubCfg, backup)
				lines, _ := utils.ReadFileLines(fields.fs, "/etc/default/grub")
				opts := getGrubCmdLineOptsLine(lines)
				assert.Len(t, opts, 3)
				assert.Contains(t, opts, "some_opt=2")
			},
		},
		{
			name:   "Shall not add the same value only flag twice",
			before: persistGrubConfig,
			fields: fields{
				grubCfg: []string{
					"GRUB_TIMEOUT=5",
					"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
					"GRUB_TERMINAL_OUTPUT=\"console\"",
					"GRUB_DEFAULT=saved",
					"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap noht rd.lvm.lv=fedora/root\"",
					"GRUB_DISABLE_SUBMENU=true",
				},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				opt: []string{"noht"},
			},
			wantErr: false,
			check: func(fields fields) {
				md5 := calcMd5(fields.grubCfg)
				backupName := fmt.Sprintf("/etc/default/grub.vectorized.%s.bk", md5)
				backupPresent, _ := afero.Exists(fields.fs, backupName)
				assert.Equal(t, backupPresent, false)
				lines, _ := utils.ReadFileLines(fields.fs, "/etc/default/grub")
				assert.Len(t, lines, 6)
				opts := getGrubCmdLineOptsLine(lines)
				assert.Len(t, opts, 3)
			},
		},
		{
			name:   "Shall update the option value if it is already present",
			before: persistGrubConfig,
			fields: fields{
				grubCfg: []string{
					"GRUB_TIMEOUT=5",
					"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
					"GRUB_TERMINAL_OUTPUT=\"console\"",
					"GRUB_DEFAULT=saved",
					"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap some_opt=1 rd.lvm.lv=fedora/root\"",
					"GRUB_DISABLE_SUBMENU=true",
				},
				fs: afero.NewMemMapFs(),
			},
			args: args{
				opt: []string{"some_opt=2"},
			},
			wantErr: false,
			check: func(fields fields) {
				md5 := calcMd5(fields.grubCfg)
				backupName := fmt.Sprintf("/etc/default/grub.vectorized.%s.bk", md5)
				lines, _ := utils.ReadFileLines(fields.fs, "/etc/default/grub")
				backup, err := utils.ReadFileLines(fields.fs, backupName)
				assert.Nil(t, err)
				assert.Equal(t, fields.grubCfg, backup)
				assert.Len(t, lines, 6)
				opts := getGrubCmdLineOptsLine(lines)
				assert.Len(t, opts, 3)
				assert.Contains(t, opts, "some_opt=2")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grub := NewGrub(nil, nil, tt.fields.fs, executors.NewDirectExecutor(), time.Duration(10)*time.Second)
			tt.before(tt.fields)
			if err := grub.AddCommandLineOptions(tt.args.opt); (err != nil) != tt.wantErr {
				t.Errorf("grub.AddCommandLineOption() error = %v, wantErr %v", err, tt.wantErr)
			}
			tt.check(tt.fields)
		})
	}
}

func Test_optionsNeedChange(t *testing.T) {
	type args struct {
		current   []string
		requested []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "shall return false for the same sets of options",
			args: args{
				current:   []string{"opt1=val1", "noht", "opt_2=val_2"},
				requested: []string{"opt1=val1", "noht", "opt_2=val_2"},
			},
			want: false,
		},
		{
			name: "shall return true as flag option differs",
			args: args{
				current:   []string{"opt1=val1", "opt_2=val_2"},
				requested: []string{"opt1=val1", "noht", "opt_2=val_2"},
			},
			want: true,
		},
		{
			name: "shall return true as key/value option differes",
			args: args{
				current:   []string{"opt1=val1", "noht", "opt_2=val_2"},
				requested: []string{"opt1=val1", "noht", "opt_2=val_3"},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := optionsNeedChange(tt.args.current, tt.args.requested); got != tt.want {
				t.Errorf("optionsNeedChange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func getGrubCmdLineOptsLine(configLines []string) []string {
	for _, line := range configLines {
		if opts := matchAndSplitCmdOptions(line); opts != nil {
			return opts
		}
	}
	return nil
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

func persistGrubConfig(fields fields) {
	utils.WriteFileLines(fields.fs, fields.grubCfg, "/etc/default/grub")
}
