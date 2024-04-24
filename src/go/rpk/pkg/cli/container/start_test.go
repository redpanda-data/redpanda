package container

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parsePort(t *testing.T) {
	tests := []struct {
		name      string
		ports     []string
		nNodes    int
		defPort   int
		exp       []uint
		expRandom bool
		expErr    bool
	}{
		{
			name:    "nil array",
			ports:   nil,
			nNodes:  3,
			defPort: 9092,
			exp:     []uint{9092, 10092, 11092},
		},
		{
			name:    "empty array",
			ports:   []string{},
			nNodes:  1,
			defPort: 9644,
			exp:     []uint{9644},
		},
		{
			name:      "any",
			ports:     []string{"any"},
			nNodes:    3,
			defPort:   8082,
			expRandom: true,
		},
		{
			name:    "parse correctly",
			ports:   []string{"33145", "33146", "33147"},
			nNodes:  3,
			defPort: 33145,
			exp:     []uint{33145, 33146, 33147},
		},
		{
			name:    "fill correctly",
			ports:   []string{"9092"},
			nNodes:  5,
			defPort: 9092,
			exp:     []uint{9092, 10092, 11092, 12092, 13092},
		},
		{
			name:    "fill correctly with different ports",
			ports:   []string{"9092", "33146"},
			nNodes:  5,
			defPort: 9092,
			exp:     []uint{9092, 33146, 34146, 35146, 36146},
		},
		{
			name:    "err if any + port",
			ports:   []string{"any", "33146"},
			nNodes:  5,
			defPort: 9092,
			expErr:  true,
		},
		{
			name:    "err if unrecognized value",
			ports:   []string{"ninetyninetytwo"},
			nNodes:  1,
			defPort: 9092,
			expErr:  true,
		},
		{
			name:    "err if negative value",
			ports:   []string{"9092", "-9093", "9094"},
			nNodes:  3,
			defPort: 9092,
			expErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePorts(tt.ports, tt.nNodes, tt.defPort)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.expRandom {
				require.Len(t, got, tt.nNodes)
				return
			}
			require.Equal(t, tt.exp, got)
		})
	}
}
