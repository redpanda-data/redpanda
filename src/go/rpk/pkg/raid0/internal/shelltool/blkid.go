// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shelltool

type blkid struct {
	shelltool
}

func BlockID(device string) *blkid {
	b := new(blkid)
	b.command = "/usr/sbin/blkid"
	b.arguments = append(b.arguments, device)

	return b
}

func (b *blkid) MatchTag(tag string) *blkid {
	b.options = append(b.options, "--match-tag", tag)
	return b
}

func (b *blkid) OutputFormat(format string) *blkid {
	b.options = append(b.options, "--output", format)
	return b
}
