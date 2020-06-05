package vendor

type Vendor interface {
	Name() string
	Init() (InitializedVendor, error)
}

type InitializedVendor interface {
	Name() string
	VmType() (string, error)
}
