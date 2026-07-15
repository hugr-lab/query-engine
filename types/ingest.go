package types

type IngestOpt func(*IngestOpts)

type IngestOpts struct {
	Mode IngestMode
	Keys []string
}

type IngestMode int

const (
	IngestModeAppend IngestMode = iota
	IngestModeReplace
	IngestModeUpsert
	IngestModeDryRun
)

func WithDryRun() IngestOpt {
	return func(opts *IngestOpts) {
		opts.Mode = IngestModeDryRun
	}
}

func WithAppend() IngestOpt {
	return func(opts *IngestOpts) {
		opts.Mode = IngestModeAppend
	}
}

func WithReplace(keys ...string) IngestOpt {
	return func(opts *IngestOpts) {
		opts.Mode = IngestModeReplace
		opts.Keys = keys
	}
}

func WithUpsert(keys ...string) IngestOpt {
	return func(opts *IngestOpts) {
		opts.Mode = IngestModeUpsert
		opts.Keys = keys
	}
}
