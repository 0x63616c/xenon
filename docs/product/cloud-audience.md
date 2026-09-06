# Xenon Cloud: first customer hypothesis

Date: 2026-09-06 UTC. Discovery ticket: #86. Status: provisional product direction and copy draft, not a launched service or validated demand. User direction: sensitive company data stays in customer-controlled infrastructure while Xenon handles running the service. Preserve the existing S3-only durability, Temporal compatibility and correctness requirements.

## Proposed first customer

A platform team already using Temporal on AWS, required to retain workflow history and visibility data in its own cloud account, with recurring work operating Temporal and its persistence layer.

Recruit initially from security software and enterprise SaaS teams handling sensitive customer workflows. These are recruitment hypotheses, not established buyers. Qualify by the actual account-boundary requirement and operational pain, not by industry labels. Finance and healthcare may also qualify, but their procurement and assurance requirements may make them poor first pilots.

The champion is the platform lead or Staff infrastructure engineer accountable for Temporal. The likely budget owner is the Head of Infrastructure or VP Engineering. Security and cloud-governance reviewers can veto a deployment even when engineering wants it. All roles need confirmation in interviews.

The buying trigger is a required move into the customer's account, or an existing self-hosted cluster consuming time through upgrades, database operations and incidents. Teams satisfied with their existing managed service are a weaker initial audience. So are greenfield users with no Temporal investment and organizations needing an immediately certified, air-gapped production platform.

## What the research establishes

These primary sources were read on the date above. Vendor pages describe their own offers; they are not independent evidence of demand for Xenon.

- [Daylight's September 1 migration account](https://daylight.ai/blog/how-we-migrated-off-temporal-cloud-without-downtime) reports moving from Temporal Cloud to self-hosting because of costs and a desire to keep sensitive workflow data within its VPC. It describes running Aurora PostgreSQL and OpenSearch and later resolving database connection-pool starvation. This is a concrete example of the problem combination, not a Xenon prospect commitment or evidence that its workload fits Xenon's performance.
- [Temporal's security documentation](https://docs.temporal.io/cloud/security) describes optional client-side payload encryption with keys outside Temporal Cloud. Therefore generic concern about vendor plaintext access is insufficient qualification: determine why these controls do not satisfy a prospect's requirements.
- [Temporal's Search Attributes documentation](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/visibility/search-attributes.mdx) says values bypass custom payload codecs and must be readable to the server for filtering. Visibility fields require an explicit data classification alongside payloads. This does not mean storage lacks infrastructure encryption.
- [Restate's BYOC announcement](https://restate.dev/blog/announcing-restate-byoc) describes managed durable execution inside customer cloud accounts. Managed BYOC is already a competitive category. Xenon's proposed distinction is retaining Temporal Server, SDKs and UI while offering customer-account deployment and managed operations, subject to compatibility proof.
- [WarpStream's BYOC page](https://www.warpstream.com/bring-your-own-cloud-kafka-data-streaming) assigns compute scheduling/running to the customer while WarpStream hosts the control plane. [Its security documentation](https://docs.warpstream.com/warpstream/reference/security-and-privacy-considerations) explicitly lists metadata leaving the data plane. BYOC does not imply a uniform management or data-access model.

The strongest alternative to beat is the buyer's actual current setup: Temporal Cloud with suitable controls, or self-hosted Temporal with a familiar database and existing staff. Do not assume changing the persistence engine is inherently an attractive purchase.

## Product hypothesis

**Managed Temporal inside your cloud account.** Xenon would operate Temporal Server and its S3-backed persistence layer. Customers would continue to own application code and workers.

S3 is an enabling architecture. The purchased outcome is less operational work while retaining the required account boundary. Cost savings, approval speed and reduced incidents must be measured before becoming claims.

| Area | Proposed Xenon responsibility | Customer responsibility / discovery question |
| --- | --- | --- |
| Service lifecycle | Deploy, monitor, upgrade and recover Temporal Server and Xenon nodes | Approve installation, account policy and maintenance windows |
| Capacity and incidents | Manage service capacity and respond to service incidents under a defined support agreement | Provide approved cloud quotas and budget; operate application workers |
| Durable state | Operate persistence, retention and tested recovery within the agreed boundary | Own the cloud account, storage and key policy; approve retention requirements |
| Authentication and networking | Maintain service endpoints and authentication integration | Own identity policy, network approvals and application credentials |
| Application execution | Support the declared Temporal compatibility surface | Write, deploy, scale and debug workflow/activity code and external integrations |

This table proposes a service contract, not a delivered feature list. “We run everything” must be narrowed to the Temporal service stack. Operating customers' application workers and business logic is a materially different product.

## Data and access boundary to prove

Customer-account storage and vendor inability to access data are distinct properties. Running vendor software or allowing upgrades can create access paths even without a standing support login.

Before advertising a residency guarantee, classify and trace workflow inputs/outputs, event histories, visibility attributes, identifiers, logs, traces, metrics labels, crash dumps and support bundles. Keep sensitive application data within the customer's agreed account/region boundary. Define any exported operational data by an explicit schema; do not assume metadata is harmless.

Choose and test an operator-access model, update authority, revocation behavior and audit trail. Specify who holds keys and which processes can decrypt. The commercial control plane must not become a hidden durability authority: S3-only application durability remains a locked requirement. Verify how the data plane behaves when management connectivity disappears.

BYOC initially means a customer's cloud account. On-premises means their datacenter and requires separate installation, networking, update and support proof. Neither deployment model automatically removes security review or compliance obligations. Avoid “no compliance issues,” “no security risk” and “we can never see your data” as unsupported claims.

## Proposed discovery before expanding the build

Conduct six to eight interviews across independent teams, starting with existing Temporal operators. No interviews or outreach have occurred; external messaging needs explicit authorization. Do not request confidential customer payloads or internal policy documents; descriptions or voluntarily sanitized examples are enough.

1. Describe the last Temporal upgrade or incident that took significant time. What did the team actually do?
2. What must remain in your account: payloads, histories, visibility fields, telemetry, or everything? Is this a written policy, customer contract requirement, or preference?
3. Why do existing hosted-service encryption and networking controls not meet that requirement?
4. Who operates Server, storage, visibility and workers today? Which responsibilities would you pay to transfer?
5. Which vendor permissions, update mechanisms and exported metrics would security accept? Who makes that decision?
6. What do operations consume today in engineering effort and infrastructure spend? Who owns a purchase and renewal?
7. What representative noncritical workflow could run in a pilot? What compatibility, latency, recovery and upgrade results would justify proceeding?
8. What would stop adoption even if the pilot worked: procurement, migration risk, assurance requirements, operator access, or price?

Record observed behavior separately from expressed interest. A compliment about the headline is not buying evidence. Quote a concrete service scope and price only after understanding operating costs and support obligations; do not fabricate a price or infer willingness to pay.

Proposed discovery gate: at least three independent teams describe both a hard account constraint and recurring operational pain; at least two identify a buyer and agree to scope a representative pilot. These are internal learning gates, not market-size statistics. A pilot starts only after the necessary runtime, access and security requirements pass. Stop or revise the hypothesis if existing alternatives satisfy policy, acceptable management access cannot be agreed, or adopting a new engine adds more risk than the operational benefit removes.

## Website draft

For the future Cloud offering, lead with the purchased outcome:

**Your cloud. Operated for you.**

*We're building managed Temporal for teams that need workflow data to stay in their own cloud account. You build the workflows. We take care of the Temporal service.*

Status: **In development.**

Supporting section ideas:

- **Keep data in your account.** Explain the proposed data boundary, ownership and approved operational access.
- **Keep building with Temporal.** Existing workflow investment is the reason to evaluate Xenon; link to actual compatibility coverage.
- **Hand over the operations.** Name deployment, upgrades, monitoring and recovery as the planned service scope, without a current SLA promise.

Alternative hero: **Managed Temporal. In your cloud.** This is more explicit about the product category. “Your data. Your cloud. We run Temporal.” is more conversational but needs the same development qualification.

For today's preview, the CTA remains **Explore the architecture**. A future **Discuss a pilot** CTA requires a real contact destination. The existing email form is not connected and cannot collect interest; do not interpret local submissions as leads or imply anyone will be notified.

Keep the engine homepage's “Temporal, backed by S3” as the technical explanation. This draft adds an outcome-led Cloud proposition without claiming the managed service exists. Website implementation can follow the agreed audience and service scope; the brief itself does not authorize a new SaaS control plane, public deployment, spending or outreach.

## Delegated decision

Delegated agent decision under Calum's autonomous-delivery authorization: use the hard-own-account Temporal operator as the provisional discovery audience. An independent user-priority advocate and adversarial systems reviewer considered the same constraints. Both favored existing Temporal users over a broad regulated-industry segment. The review highlighted existing encryption alternatives, visibility data, operational-access limits and the difficulty of selling a new engine to risk-averse buyers. The coordinator adopts a bounded noncritical pilot path and explicit service responsibility boundary.

This selects what to investigate, not a claim that Calum personally chose an AWS-only product or approved an access architecture. Revisit after the interviews, a conflicting customer constraint, or runtime evidence showing the proposed workload is unsuitable. Keep current core correctness work moving; do not launch a managed service merely because the copy is appealing.
