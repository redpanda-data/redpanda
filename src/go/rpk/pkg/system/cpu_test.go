package system_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestCpuInfo(t *testing.T) {
	defaultSetup := func(fs afero.Fs) error {
		contents := `processor	: 1
vendor_id	: GenuineIntel
cpu family	: 6
model		: 158
model name	: Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz
stepping	: 13
microcode	: 0xca
cpu MHz		: 953.249
cache size	: 16384 KB
physical id	: 0
siblings	: 16
core id		: 7
cpu cores	: 8
apicid		: 15
initial apicid	: 15
fpu		: yes
fpu_exception	: yes
cpuid level	: 22
wp		: yes
flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx pdpe1gb rdtscp lm constant_tsc art arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc cpuid aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx smx est tm2 ssse3 sdbg fma cx16 xtpr pdcm pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand lahf_lm abm 3dnowprefetch cpuid_fault epb invpcid_single ssbd ibrs ibpb stibp ibrs_enhanced tpr_shadow vnmi flexpriority ept vpid ept_ad fsgsbase tsc_adjust bmi1 avx2 smep bmi2 erms invpcid mpx rdseed adx smap clflushopt intel_pt xsaveopt xsavec xgetbv1 xsaves dtherm ida arat pln pts hwp hwp_notify hwp_act_window hwp_epp md_clear flush_l1d arch_capabilities
bugs		: spectre_v1 spectre_v2 spec_store_bypass swapgs taa itlb_multihit
bogomips	: 4599.93
clflush size	: 64
cache_alignment	: 64
address sizes	: 39 bits physical, 48 bits virtual
power management:

processor	: 2
vendor_id	: GenuineIntel
cpu family	: 6
model		: 158
model name	: Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz
stepping	: 13
microcode	: 0xca
cpu MHz		: 1311.446
cache size	: 16384 KB
physical id	: 0
siblings	: 16
core id		: 6
cpu cores	: 8
apicid		: 13
initial apicid	: 13
fpu		: yes
fpu_exception	: yes
cpuid level	: 22
wp		: yes
flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx pdpe1gb rdtscp lm constant_tsc art arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc cpuid aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx smx est tm2 ssse3 sdbg fma cx16 xtpr pdcm pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand lahf_lm abm 3dnowprefetch cpuid_fault epb invpcid_single ssbd ibrs ibpb stibp ibrs_enhanced tpr_shadow vnmi flexpriority ept vpid ept_ad fsgsbase tsc_adjust bmi1 avx2 smep bmi2 erms invpcid mpx rdseed adx smap clflushopt intel_pt xsaveopt xsavec xgetbv1 xsaves dtherm ida arat pln pts hwp hwp_notify hwp_act_window hwp_epp md_clear flush_l1d arch_capabilities
bugs		: spectre_v1 spectre_v2 spec_store_bypass swapgs taa itlb_multihit
bogomips	: 4599.93
clflush size	: 64
cache_alignment	: 64
address sizes	: 39 bits physical, 48 bits virtual
power management:

`
		return afero.WriteFile(
			fs,
			"/proc/cpuinfo",
			[]byte(contents),
			0o755,
		)
	}
	tests := []struct {
		name           string
		before         func(fs afero.Fs) error
		expectedErrMsg string
		expected       []*system.CPUInfo
	}{{
		name:           "it should fail if /proc/cpuinfo is missing",
		expectedErrMsg: "/proc/cpuinfo",
	}, {
		name:           "it should fail if /proc/cpuinfo is empty",
		expectedErrMsg: "/proc/cpuinfo is empty",
		before: func(fs afero.Fs) error {
			return afero.WriteFile(
				fs,
				"/proc/cpuinfo",
				[]byte(""),
				0o755,
			)
		},
	}, {
		name: "it should parse the CPU model & # of cores",
		expected: []*system.CPUInfo{{
			ModelName: "Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz",
			Cores:     8,
		}, {
			ModelName: "Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz",
			Cores:     8,
		}},
		before: defaultSetup,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.before != nil {
				require.NoError(st, tt.before(fs))
			}
			cpus, err := system.GetCPUInfo(fs)
			if tt.expectedErrMsg != "" {
				require.Error(st, err)
				require.Contains(st, err.Error(), tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
			require.Exactly(st, tt.expected, cpus)
		})
	}
}
