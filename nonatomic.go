package spark

// NonAtomic marks a command whose save+publish is NOT committed as one atomic
// unit of work: middleware.Atomic (see spark/middleware, via IsNonAtomic) skips the
// transaction for it. Compose it like Result:
//
//	type GenerateReply struct {
//	    spark.NonAtomic
//	    spark.Result[*Reply]
//	    ConversationID string
//	}
//
// Use it for reads and for handlers that do external I/O or long-lived/streaming
// calls that must not run inside a transaction. Default (not embedding NonAtomic)
// is atomic (wrapped in the Atomic transaction).
type NonAtomic struct{}

func (NonAtomic) isNonAtomic() {}

// IsNonAtomic reports whether cmd embeds NonAtomic. The marker method is
// unexported, so only embedding spark.NonAtomic satisfies it (same sealing as
// Result's resultType); detection lives here because the method identity is
// scoped to spark.
func IsNonAtomic(cmd Command) bool {
	_, ok := cmd.(interface{ isNonAtomic() })
	return ok
}
