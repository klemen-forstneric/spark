package spark

// ReadOnly is an optional marker a command implements to declare that it makes
// no writes. Transaction-scoping middleware (see middleware.UnitOfWork) skips
// opening a transaction for such commands. Mirrors the optional Keyed
// interface. Default (unmarked) is treated as a write, so a forgotten marker
// fails safe — an extra transaction, never lost atomicity.
type ReadOnly interface {
	ReadOnly()
}
