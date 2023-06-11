// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package out

import (
	"strings"

	"github.com/fatih/color"
)

// ParseColor parses a string into a color.Attribute. Foreground colors are
// simply named ("red", "black"), highlights have a "hi-" prefix, and
// background colors have an additional "bg-" prefix. Bold, faint, underline,
// and invert attributes are also supported by name directly.
func ParseColor(c string) (color.Attribute, bool) {
	switch strings.ToLower(strings.TrimSpace(c)) {
	case "black":
		return color.FgBlack, true
	case "red":
		return color.FgRed, true
	case "green":
		return color.FgGreen, true
	case "yellow":
		return color.FgYellow, true
	case "blue":
		return color.FgBlue, true
	case "magenta":
		return color.FgMagenta, true
	case "cyan":
		return color.FgCyan, true
	case "white":
		return color.FgWhite, true
	case "hi-black":
		return color.FgHiBlack, true
	case "hi-red":
		return color.FgHiRed, true
	case "hi-green":
		return color.FgHiGreen, true
	case "hi-yellow":
		return color.FgHiYellow, true
	case "hi-blue":
		return color.FgHiBlue, true
	case "hi-magenta":
		return color.FgHiMagenta, true
	case "hi-cyan":
		return color.FgHiCyan, true
	case "hi-white":
		return color.FgHiWhite, true
	case "bg-black":
		return color.BgBlack, true
	case "bg-red":
		return color.BgRed, true
	case "bg-green":
		return color.BgGreen, true
	case "bg-yellow":
		return color.BgYellow, true
	case "bg-blue":
		return color.BgBlue, true
	case "bg-magenta":
		return color.BgMagenta, true
	case "bg-cyan":
		return color.BgCyan, true
	case "bg-white":
		return color.BgWhite, true
	case "bg-hi-black":
		return color.BgHiBlack, true
	case "bg-hi-red":
		return color.BgHiRed, true
	case "bg-hi-green":
		return color.BgHiGreen, true
	case "bg-hi-yellow":
		return color.BgHiYellow, true
	case "bg-hi-blue":
		return color.BgHiBlue, true
	case "bg-hi-magenta":
		return color.BgHiMagenta, true
	case "bg-hi-cyan":
		return color.BgHiCyan, true
	case "bg-hi-white":
		return color.BgHiWhite, true
	case "bold":
		return color.Bold, true
	case "faint":
		return color.Faint, true
	case "underline":
		return color.Underline, true
	case "invert":
		return color.ReverseVideo, true
	default:
		return 0, false
	}
}
