<script setup>
import { ref, computed } from "vue";
const view = ref("path"),
  selected = ref(0);
const views = {
  path: {
    label: "Request path",
    nodes: [
      [
        "SDKs, Omes & UI",
        "Public Temporal APIs",
        "The familiar surface",
        "Applications and Omes use Temporal SDKs. The unchanged Temporal UI talks to Temporal’s frontend API. None of these clients speaks directly to SlateDB or stores application data in local Xenon files.",
      ],
      [
        "Temporal instances",
        "Engine + persistence adapters",
        "The engine stays in place",
        "Multiple Temporal processes run their normal frontend, history, matching and worker services. Xenon factories implement the pinned execution and visibility persistence interfaces. Adapters encode complete operations with stable partition and operation identities.",
      ],
      [
        "One Xenon service",
        "Stable ingress",
        "One endpoint, any node",
        "Adapters connect to one service address. The local proof uses HAProxy. Kubernetes Service is the intended deployment equivalent, not a separately verified Kubernetes installation. An ingress node forwards if another node owns the partition.",
      ],
      [
        "Go storage nodes",
        "Local execution or forwarding",
        "Routing is not authority",
        "Each process can own several partitions. The router resolves READY ownership, preserves the original request envelope, and bounds forwarding and retries. A route does not grant write authority: the owning node checks fresh S3 records inside admission.",
      ],
      [
        "Partition owner + SlateDB",
        "Atomic operation and outcome",
        "One owner per storage partition",
        "A partition has a shared admission gate and its own SlateDB handle. The handler checks conditions and records data plus its durable result in one transaction. A nonempty durable barrier checks fencing before publishing a result.",
      ],
      [
        "S3 object storage",
        "Data + directory + topology",
        "Durable state outlives compute",
        "SlateDB persists application state directly to S3. Conditional S3 objects record topology intentions and owner generations. Metadata prefixes are disjoint from engine data prefixes. Local caches can be rebuilt; S3 is the durable authority.",
      ],
    ],
  },
  node: {
    label: "Inside a node",
    nodes: [
      [
        "Typed gRPC dispatcher",
        "Complete persistence operations",
        "Preserve the operation",
        "The same Go binary registers each persistence family. Dispatch selects a stable logical partition; forwarding preserves operation ID, input digest and typed request bytes. Addresses do not enter durable application keys or pagination tokens.",
      ],
      [
        "Bounded admission",
        "One gate per partition",
        "Check ownership inside the gate",
        "The owner requires fresh topology activation for this exact process incarnation and an exact READY directory identity. Stale, opening or quarantined owners reject new work. Different partitions have separate gates and native handles.",
      ],
      [
        "Durable outcome lookup",
        "Request ID + digest",
        "Replay the same result",
        "A repeated operation with the same identity and digest returns its recorded result. Reusing an identity for different input is rejected. The journal is retained and explicitly capacity bounded; automatic outcome garbage collection is not implemented.",
      ],
      [
        "Serializable transaction",
        "Conditions + data + outcome",
        "Keep transaction boundaries intact",
        "Handlers validate their pinned persistence conditions, apply their data changes, and journal the result atomically. Logical conflicts retain typed semantics. Native uncertainty is a different disposition and quarantines the owner.",
      ],
      [
        "Durability + fence barrier",
        "Nonempty native write",
        "Do not publish a captured result early",
        "The managed owner performs a unique nonempty write to a reserved overwritten key and awaits durability before returning. This also fences reads and replay. An empty transaction or a flush alone cannot prove current writer authority.",
      ],
      [
        "Reply or quarantine",
        "Retain in-flight resources",
        "Uncertainty stops admission",
        "When native work times out, its resources remain alive until completion. The owner rejects subsequent admissions rather than destroying a handle while native work is still running. A fresh reservation is required for recovery.",
      ],
    ],
  },
  recovery: {
    label: "Move & recover",
    nodes: [
      [
        "Publish topology intention",
        "Conditional S3 update",
        "An explicit administrative move",
        "The administrator assigns a partition to a specific activated process incarnation. This is an explicit S3 compare-and-swap operation, not an automatic election, heartbeat lease or failure detector. A replacement process receives a new incarnation.",
      ],
      [
        "Reserve a generation",
        "One-shot opening attempt",
        "Never reuse an old attempt",
        "The desired owner claims a fresh directory generation and transition UUID using exact object conditions. It opens SlateDB once for that reservation. The data prefix is bound to the directory and cannot be supplied by the incoming request.",
      ],
      [
        "Open from S3",
        "Recover + fence",
        "Recover durable engine state",
        "The new handle recovers SlateDB state and fences an older writer. A delayed obsolete opener can later fence this handle too; the protocol must retire and recover again. Finite delayed contenders are tested, not arbitrary perpetual contention.",
      ],
      [
        "Publish READY",
        "Recheck activation + exact CAS",
        "Two objects are not one transaction",
        "Topology and directory updates are separate conditional objects. A topology-only race may briefly leave an obsolete READY record. Fresh activation checks under the operation gate prevent that record from authorizing new application work.",
      ],
      [
        "Refresh route + retry",
        "Same operation identity",
        "Resolve uncertainty through replay",
        "Clients continue through the stable service. Forwarding refreshes stale ownership and preserves the original operation. Durable outcomes reconcile a lost response without applying a second mutation. Deadlines still bound how long callers wait.",
      ],
      [
        "Verify recovered state",
        "History + visibility + evidence",
        "Recovery must be observed",
        "The smoke restarts nodes with fresh local runtime directories and checks saved workflow histories, results and visibility. Its recorded smoke passed exact history recovery but failed the post-cold Omes visibility deadline. Full recovery acceptance remains open.",
      ],
    ],
  },
};
const nodes = computed(() => views[view.value].nodes);
const current = computed(() => nodes.value[selected.value]);
function change(key) {
  view.value = key;
  selected.value = 0;
}
function tabKey(event) {
  const keys = Object.keys(views);
  let i = keys.indexOf(view.value);
  if (event.key === "ArrowRight") i = (i + 1) % keys.length;
  else if (event.key === "ArrowLeft") i = (i + keys.length - 1) % keys.length;
  else if (event.key === "Home") i = 0;
  else if (event.key === "End") i = keys.length - 1;
  else return;
  event.preventDefault();
  change(keys[i]);
  event.currentTarget.parentElement.children[i].focus();
}
</script>
<template>
  <div class="architecture-explorer">
    <div class="arch-tabs" role="tablist" aria-label="Architecture view">
      <button
        v-for="(entry, key) in views"
        :key="key"
        role="tab"
        :aria-selected="view === key"
        :tabindex="view === key ? 0 : -1"
        @keydown="tabKey"
        @click="change(key)"
      >
        {{ entry.label }}
      </button>
    </div>
    <div class="arch-stage" :aria-label="views[view].label">
      <template v-for="(node, index) in nodes" :key="view + index"
        ><button
          class="arch-node"
          :class="{ selected: index === selected, engine: index === 4 }"
          :aria-pressed="index === selected"
          @click="selected = index"
        >
          <strong>{{ node[0] }}</strong
          ><span>{{ node[1] }}</span>
        </button>
        <div
          v-if="index < nodes.length - 1"
          class="arch-arrow"
          aria-hidden="true"
        >
          ↓
        </div></template
      >
    </div>
    <div class="arch-inspector" aria-live="polite">
      <span class="eyebrow"
        >{{ String(selected + 1).padStart(2, "0") }} /
        {{ views[view].label }}</span
      >
      <h3>{{ current[2] }}</h3>
      <p>{{ current[3] }}</p>
    </div>
    <p class="arch-scope">
      Conceptual request and recovery paths. This guide does not run a cluster.
    </p>
  </div>
</template>
