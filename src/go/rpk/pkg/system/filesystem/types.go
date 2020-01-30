package filesystem

type FsType string

const (
	Xfs     FsType = "xfs"
	Ext     FsType = "ext"
	Tmpfs   FsType = "tmpfs"
	Hfs     FsType = "hfs"
	Unknown FsType = "unknown"
)
