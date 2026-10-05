package multistmt

// Option configures a single Batch.Execute call.
type Option func(*execOptions)

type execOptions struct {
	preparedCache *PreparedCache
}

// WithPreparedCache makes Execute automatically manage server-side PREPARE
// reuse for this batch through cache, instead of generating a fresh PREPARE
// for every statement and DEALLOCATEing it at the end of this one call.
//
//	err := b.Execute(ctx, conn, multistmt.WithPreparedCache(cache))
//
// See PreparedCache's doc comment for how it decides what's safe to reuse —
// in short, it's keyed by conn's underlying physical connection, so it keeps
// working across database/sql pool checkouts even though conn itself is a
// fresh *sql.Conn object every time.
//
// Statements that set Statement.PreparedName themselves are left untouched:
// cache only manages statements that don't.
func WithPreparedCache(cache *PreparedCache) Option {
	return func(o *execOptions) {
		o.preparedCache = cache
	}
}
