# Fair matching contract

Temporal v1.31.2 at 19a774302c613da9adc4436ab14278ccdca8e0a5 defines TaskStore. NewFairMatchingStore uses the same thirteen methods, distinct fair queue/task keys, and shared namespace user data. Fair requests require matching protocol 2; legacy requests remain protocol 1. Old nodes reject version 2, preventing unknown protobuf fields from silently selecting legacy storage.

Tasks are ordered by the signed (pass, task ID) tuple within queue/subqueue. Cursors bind the queue and original lower bound and resume strictly after the exact tuple, without adding one. Completion removes at most the requested number of exact keys strictly below the exclusive maximum tuple.

Three deliberate corrections to the pinned PostgreSQL implementation were independently reviewed: its SELECT omits pass although pagination needs it; its manager adds one to task ID and can overflow MaxInt64; its DELETE identifies rows by task ID alone and can remove another pass beyond the requested limit. Xenon preserves the intended tuple contract and tests these boundaries. Explicit read bounds otherwise follow the pinned fair API: minimum pass at least one and maximum task ID equal to MaxInt64.

Run `python3 scripts/prove.py go-fair` from a clean checkout. The committed manifest records inputs, tools, native library and binary provenance and exact expected upstream test events. Tests use the official native memory object store. The upstream suite helper declares a 1ms WAL flush to fit upstream deadlines; production defaults remain unchanged. This is not an S3, Temporal boot, or cluster routing proof.
