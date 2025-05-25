package theatre

type HostOptions struct {
	Mode HostMode
}

type HostOption func(*HostOptions)

func WithHostMode(mode HostMode) HostOption {
	return func(o *HostOptions) {
		o.Mode = mode
	}
}
