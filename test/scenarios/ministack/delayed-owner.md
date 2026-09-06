# One live delayed stale opener

Status: NOT_EXECUTED. Run `python3 scripts/ministack-runtime.py --delayed-owner`.
This is a bounded smoke fault composition, not the full acceptance gate.

The optional node-C filesystem control pauses the real matching reservation
between successful S3 Reserve and native Build. It captures the full reservation
and a fresh controller session, including the actual node boot incarnation.
Only an atomic exact release accepts; a120-second timeout records FAILED and
returns without Build. This process-local hook provides no authority. Default
nodes do not install it. One hook is consumed once and preserves its first
reservation receipt across later normal reconciliations.

While real Omes work remains live, the controller reassigns matching to its
actual prior owner and observes READY at a newer generation plus local dispatch.
It releases C, observes real Build success and completed retirement caused by
superseded topology before READY publication (no claim of a READY CAS attempt),
then requires the prior owner's still newer generation plus positive dispatch
within120 seconds of release. C must have zero matching dispatch in this phase.
Only after that evidence does a fresh assignment let C serve normal matching.
Existing acknowledged workflow, Omes20, UI, cold recovery and outcome-capacity
checks remain required. Their runtime receipt cannot be replaced by component
controls. The new reservation's success does not substitute for prior-owner
recovery. Native Build/Shutdown can still block: existing retained-handle rules
apply, and the external bounded controller fails rather than fabricating progress.

Clean component controls: `python3 scripts/prove.py delayed-owner`.
No change to frozen full-profile counts, deadlines or storage authority.
