# Xenon website copy

Research and revision: 2026-09-06 UTC. Work tracked in #84; based on the private website integration at `32ba174`. This is editorial research, not measured conversion research.

## Audience and purpose

Write for engineers evaluating storage for self-hosted Temporal. They already know why they use workflows. Their next questions are what Xenon changes, how it stores state, and what evidence supports recovery. The primary action is **Explore the architecture**; the secondary action is **Read the docs**. Both destinations exist. Xenon is in development and Cloud is unavailable.

## Reference websites

Read the live homepages and inspected their first-screen browser rendering. The observations below describe presentation, not independently verified product capabilities or conversion results.

| Reference | Sentence and page structure | Application to Xenon |
| --- | --- | --- |
| [WarpStream](https://www.warpstream.com/) | Names compatibility and the product category in its headline. The supporting paragraph explains the storage mechanism, then its operational implications. Architecture comparisons and client code make the explanation concrete. | Put Temporal and S3 in the headline. Explain the persistence integration immediately afterward. Keep the system diagram close to that explanation. |
| [Restate](https://restate.dev/) | Opens with a developer action and desired outcome. A short product definition follows. Later sections use code and a failure animation to explain behavior; action labels lead directly to documentation. | Use verbs in feature headings. Describe a write and a failure in familiar language before sending readers into the technical guide. |
| [turbopuffer](https://turbopuffer.com/) | A three-word headline leads into a literal product description. A compact architecture diagram sits beside the copy. Customer evidence, a calculator and limits follow. | Keep the hero compact and let the diagram explain topology. Give evidence its own clear section. Xenon's available evidence is a bounded local recovery test. |

The shared pattern is proposition → concrete explanation → visual mechanism → evidence → useful next action. These sites have different voices; Xenon uses their clarity and hierarchy in original wording. No competitor imagery, testimonials, pricing, performance figures or distinctive slogans are reused. A website inspection cannot show which wording converts best.

## Voice

An infrastructure engineer explaining a useful system to another engineer. Calm, direct, technically specific. Give the reader the point before the implementation detail.

- Name the product relationship: Xenon provides persistence beneath Temporal Server.
- Use an action in feature headings: keep, store, connect, follow, read.
- Let a short headline set the subject; use one or two complete sentences to explain it.
- Prefer concrete subjects: Temporal connects, Xenon routes, S3 stores.
- Explain the consequence of a mechanism. Disposable local state matters because recovery starts from object storage.
- Keep ownership epochs, admission gates and replay contracts in the linked architecture guide.
- Use the status section for bounded test results and remaining validation. Preserve the development label above the hero.
- Avoid unsupported language such as drop-in replacement, full compatibility, production-ready, infinite scaling, automatic rebalancing or quantified savings.
- Match a CTA to what its destination provides. Do not advertise a trial, download or active signup that does not exist.

## Implemented page narrative

1. **Temporal, backed by S3.** Identifies the familiar system and the new storage foundation in five words. The supporting copy defines Xenon and names the retained engine, SDKs and UI.
2. **Your workflows. A new foundation.** Three parallel action headings explain integration, durable storage and the single endpoint.
3. **Follow a write. All the way to S3.** Introduces the interactive architecture through an operation the reader understands.
4. **Kill a node. Check what survives.** Describes a specific local recovery experiment and links to its results. The paragraph states that full acceptance and real AWS validation remain open.
5. **Xenon Cloud™ — Coming soon.** Retains the existing separate future-offering route. No new launch or signup promise.

Footer wording and page metadata use the same positioning. Engineering articles and detailed guides retain their technical voice.

## Headline and CTA alternatives

| Option | When to use it |
| --- | --- |
| **Temporal, backed by S3.** (selected) | The clearest compact introduction for an audience already familiar with Temporal. |
| **Object storage for Temporal.** | A more literal category heading if readers mistake Xenon for a workflow engine. |
| **A new storage foundation for Temporal.** | More explanatory, but longer and less specific about S3. |

Primary CTA alternatives: **Explore the architecture** (selected; overview and interactive guide), **See how Xenon works** (more conversational), **Follow a request** (used on the architecture teaser). All lead to the existing architecture page. These are editorial alternatives, not A/B-tested winners.

## Claim traceability

| Homepage statement | Basis and boundary |
| --- | --- |
| Temporal with S3-backed persistence | Accepted direct-S3/SlateDB architecture in AGENTS.md and [architecture guide](../docs/guide/docs/architecture.md). Does not imply completed real-AWS validation. |
| Retained engine, SDKs and UI; execution and search | Existing adapter and documented SDK/UI smoke coverage. [Status](../docs/guide/docs/status.md) expressly limits compatibility; no universal drop-in claim. |
| Disposable local state and recovery | Status records cold restart with S3 retained and identical history hashes in the declared local scenario. |
| One service address and ready-node forwarding | Accepted service routing in AGENTS.md and architecture guide. Does not imply interchangeable writers or automatic rebalancing. |
| Process killed after durable write; histories matched | Status identifies source `3076b15`, receipt `20260905T235225Z-xenon-ministack-e1a79483dbf2`. This copy pass did not rerun the runtime experiment. |

## Skills applied

Installed `copywriting` and `copy-editing` from [coreyhaines31/marketingskills](https://github.com/coreyhaines31/marketingskills/tree/5b2c0007766c6a1cf1d53fd8fc73e979e0821022/skills), pinned to `5b2c0007766c6a1cf1d53fd8fc73e979e0821022`, with Codex's skill installer. Read and applied the instructions: single proposition, explicit audience and action, one idea per section, clear benefits, and editing sweeps for clarity, voice, relevance, evidence, specificity and CTA expectations. No skill conversion statistics were adopted as evidence. Used the existing Vue components for text changes under the Ponytail minimal-change guidance.

An independent copy review checks UX clarity, infrastructure audience, voice and claim accuracy. The existing locked website build and responsive browser suite validate rendering and navigation; they do not measure conversion or runtime correctness. QA receipts stay in `.local/`, outside deployable site assets.

Independent review found two accuracy issues, both corrected: ownership metadata goes directly to S3 rather than through SlateDB, and real AWS validation is pending access rather than currently executing. Review found the hero, active headings, local test summary and destination-specific CTAs clear.
