Clean integrated component proof at `6d579a1215c71fc444c5b6aeb1dd1b22760d43c1`.
The committed script built the real CLI, verified actual dependency/native build
identities, completed four generated cases with four distinct orders, and replayed
the last case with byte-identical trace and expanded input. Continuous search
returned budget exit 2 with zero completed cases in its 250ms budget. Three Python
negative controls passed separately. All referenced local file hashes were
verified after completion.

Raw evidence is retained at `/private/tmp/xenon-generated-cli-proof-6d579a1`.
This checkpoint archives the receipt only; host paths and raw logs are not
portable archived evidence. Reproduce the complete outputs with the command in
`test/scenarios/simulation/cli-generated-search.md`. This narrow component result
does not certify the complete DST or #119 acceptance gates.
