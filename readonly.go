package spark

// ReadOnly is embedded in a command to declare it makes no writes.
// Transaction-scoping middleware (see middleware.UnitOfWork, via IsReadOnly)
// skips opening a transaction for such commands. Compose it like Result:
//
//	type GetOrder struct {
//	    spark.ReadOnly
//	    spark.Result[*Order]
//	    ID string
//	}
//
// Default (not embedding ReadOnly) is treated as a write, so a forgotten marker
// fails safe — an extra transaction, never lost atomicity.
type ReadOnly struct{}

func (ReadOnly) isReadOnly() {}

// IsReadOnly reports whether cmd embeds ReadOnly. The marker method is
// unexported, so only embedding spark.ReadOnly satisfies it — accidental
// conformance is impossible (same sealing technique as Result's resultType).
// Detection lives here, not in the middleware package, because an unexported
// method's identity is scoped to spark.
func IsReadOnly(cmd Command) bool {
	_, ok := cmd.(interface{ isReadOnly() })
	return ok
}
