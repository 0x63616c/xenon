<script setup>
import { ref, computed } from "vue";
const props = defineProps({ initialView: { type: String, default: "path" } });
const view = ref(
    ["path", "node", "recovery"].includes(props.initialView)
      ? props.initialView
      : "path",
  ),
  selected = ref(0);
const views = {
  path: {
    label: "Request path",
    nodes: [
      [
        "SDKs, Omes & UI",
        "Public Temporal APIs",
        "The familiar surface stays the same",
        "Applications and Omes call Temporal’s frontend API. Existing SDK/UI behavior is unchanged; no client writes directly to local Xenon files.",
      ],
      [
        "Unified Xenon endpoint",
        "One ingress across identical agents",
        "One service, one ingress",
        "Customers point existing Temporal clients at one Xenon service address. A load balancer or proxy fans traffic out across ready identical agents.",
      ],
      [
        "Xenon agent receives request",
        "Unified agent binary",
        "Routing is not authority",
        "The same Go binary hosts Temporal services and the Xenon runtime. It resolves ownership, then executes locally or forwards to the owner.",
      ],
      [
        "Xenon agent",
        "Local execution or forwarding",
        "Routing is not authority",
        "Each process can own multiple partitions. The router preserves envelope identity and deadlines, then routes to the owning process when needed.",
      ],
      [
        "Partition owner",
        "Atomic operation and outcome",
        "One owner per storage partition",
        "A partition has a shared admission gate and durable outcome handling. The current engine records conditions, mutation and result in one flow, then verifies writer fencing before publishing.",
      ],
      [
        "S3 object storage",
        "State + topology + membership heartbeat",
        "Durable state outlives compute",
        "SlateDB persists application data directly to S3. Conditional objects store topology intent, owner generations, and heartbeat metadata used for bounded membership eviction.",
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
        "Topology intent assigns partitions to specific activated incarnations. The target is explicit; heartbeated CAS membership handles failed-member eviction automatically.",
      ],
      [
        "Reserve a generation",
        "One-shot opening attempt",
        "Never reuse an old attempt",
        "The desired owner claims a fresh directory generation and transition UUID using exact object conditions. It opens once for that reservation. The data prefix is bound to directory ownership, not caller input.",
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
        "The smoke restarts nodes with fresh local runtime directories and checks saved workflow histories, results and visibility. Full Omes and shipping acceptance remain open while the latest local evidence confirms baseline recovery behavior.",
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
