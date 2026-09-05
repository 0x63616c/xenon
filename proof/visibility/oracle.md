# PostgreSQL Text compatibility oracle

Run `python3 scripts/visibility-oracle.py` from a clean committed checkout with the exact tools in `oracle-pins.json`. The runner starts a uniquely named, network-isolated PostgreSQL container with tmpfs-only data, checks its version, C collation and UTF8 encoding, recomputes every committed result, runs the Go matcher against the same cases, records source/input/query/image hashes and exact outputs, then removes the container even on failure. SQL is an ephemeral test oracle, never Xenon application storage. The initial supported proof environment is the pinned Linux ARM64 image on Docker 29.4.0 with the recorded Go/Python versions; other host toolchains are not claimed by this artifact.

The fixture contains 2,342 exact vector/query/result cases: the original 25-vector by 45-query product, 1,200 nested Boolean/phrase combinations generated with seed 20260905 (inputs are saved, no runtime random generation), and lexeme/position/distance boundaries. It covers missing positions, mixed positioned/stripped prefix matches, weight filtering and normalization, quotes and escaping, C whitespace, Unicode bytes, case sensitivity, compact operators, right alignment of phrase alternatives, negation within phrases, malformed input and integer limits. This is bounded differential evidence, not an exhaustive proof of every possible input. New mismatches must become committed regressions; no fallback to PostgreSQL is permitted.

Primary source pins:

- Temporal `19a774302c613da9adc4436ab14278ccdca8e0a5`, `common/persistence/visibility/store/query/util.go`: split query by ASCII spaces, drop empty pieces, then PostgreSQL converter joins pieces with ` | `. Quoted spaces are therefore transformed too; no English analyzer or stemming is applied.
- PostgreSQL `7d3e000c5961a544302072058a1184e9a588837b` (REL_16_15), `src/backend/utils/adt/tsvector_parser.c`, `tsquery.c`, `tsvector.c`, `tsvector_op.c`, and `src/include/tsearch/ts_type.h`: vector/query lexical grammar, precedence, positions/weights, and positional three-state evaluation. The Go implementation is original code following those semantics. PostgreSQL source is distributed under its PostgreSQL License.

`TextMatch(vector, query)` accepts raw tsvector input plus Temporal's original query text. It preserves case, interprets prefix/weights and compact query operators, and resolves missing-position uncertainty at the outer phrase operator before Boolean NOT, matching PostgreSQL. The position-width truncations are intentional source compatibility, not mathematical phrase-distance simplifications. Query parsing has an explicit 1,024-node admission ceiling to bound recursion; this is a resource ceiling rather than an unsupported operator. Error strings are not promised identical; oracle assertions compare success/error classification and successful match results.

The 1,200 saved nested inputs exercise the complete supported operator vocabulary but are not statistical reliability evidence. Visibility null/UNKNOWN semantics, KeywordList JSON operators, grouping, timestamp/numeric ordering, aliases/CHASM, durable indexing and full query integration are separate proofs owned by the visibility component. This matcher alone does not establish those gates.


## Scalar oracle extension

`oracle-scalars.json` freezes generated-column conversion, comparison and raw-response distinctions against the same ephemeral PG16.15/C/UTF8 instance. These are SQL oracle fixtures, not a claim the Xenon evaluator already matches them. The runner validates all expected JSON results and records scalar SQL/input hashes alongside the Text corpus results.

Custom Double JSON is cast through decimal into DECIMAL(20,5): ties round away from zero and overflow raises SQLSTATE22003. The stored generated1.23457 does not equal the unrounded query operand1.234565; SELECT search_attributes still returns the original1.234565 JSON number. Do not normalize response payloads to generated columns.

Custom Datetime's `s::timestamptz AT TIME ZONE 'UTC'` rounds fractional microseconds, with the checked ties-to-even examples .0000005→.000000, .0000015→.000002, .0000025→.000002. .9999995 carries into the next second, including the tested preepoch boundary. Offset conversion is UTC; raw search_attributes retains the original string. System timestamp columns follow Temporal's explicit UTC.Truncate(microsecond) preprocessing instead; these custom-column fixtures do not authorize changing system timestamp semantics.

KeywordList missing field is SQL NULL: containment and its NOT both return UNKNOWN/null. Explicit JSON null and an empty array yield false containment/true NOT. IN's disjunction retains UNKNOWN for missing fields. Duplicates do not change containment. C collation orders the checked UTF8 strings A,a,z,é; GROUP BY includes the NULL group and counts both missing entries. This finite fixture set is not exhaustive PostgreSQL parity evidence.
