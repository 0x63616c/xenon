<script setup>
import { ref, computed } from "vue";
import SystemDiagram from "./SystemDiagram.vue";
import Icon from "./ArchitectureIcon.vue";
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
        "S3-compatible object storage",
        "Data + directory + topology",
        "Durable state outlives compute",
        "SlateDB persists application state directly to object storage. Xenon currently uses the S3 API for both engine data and conditional ownership/topology objects. MinIO is used by the local proofs; real Amazon S3 and other providers still need their own acceptance runs. SlateDB supports additional object-store APIs, but Xenon has not implemented the corresponding ownership layers. Local disks remain disposable.",
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
        :id="`architecture-tab-${key}`"
        aria-controls="architecture-panel"
        role="tab"
        :aria-selected="view === key"
        :tabindex="view === key ? 0 : -1"
        @keydown="tabKey"
        @click="change(key)"
      >
        {{ entry.label }}
      </button>
    </div>
    <div
      id="architecture-panel"
      class="architecture-canvas"
      role="tabpanel"
      :aria-labelledby="`architecture-tab-${view}`"
    >
      <SystemDiagram
        v-if="view === 'path'"
        interactive
        :selected="selected"
        @select="selected = $event"
      />
      <div v-else-if="view === 'node'" class="node-interior">
        <div class="diagram-caption">
          <Icon name="server" />
          <div>
            <strong>Inside one Go node</strong
            ><span
              >One admission gate and SlateDB handle per owned partition.</span
            >
          </div>
        </div>
        <button
          class="operation-envelope map-hotspot"
          :class="{ chosen: selected === 0 }"
          data-stage="0"
          :aria-pressed="selected === 0"
          @click="selected = 0"
        >
          <Icon name="packet" /><span
            ><strong>A complete operation arrives</strong
            ><small>Typed gRPC request · identity + input digest</small></span
          >
        </button>
        <div class="flow-stem" aria-hidden="true">↓</div>
        <button
          class="admission-gate map-hotspot"
          :class="{ chosen: selected === 1 }"
          data-stage="1"
          :aria-pressed="selected === 1"
          @click="selected = 1"
        >
          <Icon name="gate" /><span
            ><strong>Ownership gate</strong
            ><small>Fresh authority required before admission</small></span
          ><span class="gate-rule" aria-hidden="true"></span>
        </button>
        <div class="flow-stem" aria-hidden="true">↓</div>
        <button
          class="outcome-journal map-hotspot"
          :class="{ chosen: selected === 2 }"
          data-stage="2"
          :aria-pressed="selected === 2"
          @click="selected = 2"
        >
          <Icon name="journal" /><span
            ><strong>Have we seen this operation?</strong
            ><small
              >Look up its durable outcome by identity + digest.</small
            ></span
          >
        </button>
        <div class="operation-branches">
          <div>
            <span class="branch-label">NEW OPERATION ↓</span
            ><button
              class="transaction-sheet map-hotspot"
              :class="{ chosen: selected === 3 }"
              data-stage="3"
              :aria-pressed="selected === 3"
              @click="selected = 3"
            >
              <span class="sheet-heading"
                ><Icon name="layers" /><strong>One transaction</strong></span
              ><span>Check conditions</span><span>Apply state changes</span
              ><span>Record the outcome</span><small>Atomic in SlateDB</small>
            </button>
          </div>
          <div class="replay-branch">
            <span class="branch-label">SAME ID + DIGEST ↓</span
            ><span class="replay-result"
              ><Icon name="document" /><strong>Recorded result</strong
              ><small>Skip reapplying the mutation</small></span
            ><span class="replay-line" aria-hidden="true"></span>
          </div>
        </div>
        <div class="branch-merge" aria-hidden="true"></div>
        <button
          class="durability-barrier map-hotspot"
          :class="{ chosen: selected === 4 }"
          data-stage="4"
          :aria-pressed="selected === 4"
          @click="selected = 4"
        >
          <Icon name="shield" /><span
            ><strong>Wait for durability + check fencing</strong
            ><small>Required for new work, reads and replay.</small></span
          >
        </button>
        <div class="flow-stem" aria-hidden="true">↓</div>
        <button
          class="operation-result map-hotspot"
          :class="{ chosen: selected === 5 }"
          data-stage="5"
          :aria-pressed="selected === 5"
          @click="selected = 5"
        >
          <span><Icon name="check" /><strong>Reply when safe</strong></span
          ><span>Uncertain native work?<br /><b>Quarantine the owner.</b></span>
        </button>
      </div>
      <div v-else class="recovery-map">
        <div class="diagram-caption">
          <Icon name="route" />
          <div>
            <strong>Move a partition, keep its identity</strong
            ><span
              >Explicit administration starts a move. This is not an automatic
              election.</span
            >
          </div>
        </div>
        <button
          class="admin-command map-hotspot"
          :class="{ chosen: selected === 0 }"
          data-stage="0"
          :aria-pressed="selected === 0"
          @click="selected = 0"
        >
          <Icon name="terminal" /><span
            ><small>TOPOLOGY INTENTION</small
            ><strong>Assign partition P2 to node B</strong></span
          >
        </button>
        <div class="flow-stem" aria-hidden="true">↓</div>
        <button
          class="ownership-record map-hotspot"
          :class="{ chosen: selected === 1 }"
          data-stage="1"
          :aria-pressed="selected === 1"
          @click="selected = 1"
        >
          <Icon name="document" /><span
            ><strong>Reserve a fresh generation</strong
            ><small>Conditional ownership record in object storage</small></span
          ><span class="generation-stamp">n → n+1</span>
        </button>
        <div class="migration-scene">
          <div class="retired-node">
            <Icon name="server" /><strong>Node A</strong
            ><span class="partition-chip">P2</span
            ><small>Old writer is fenced<br />by the new opener.</small>
          </div>
          <div class="migration-arrow">
            <span>same P2</span><b aria-hidden="true">→</b
            ><small>State is recovered<br />from object storage.</small>
          </div>
          <div class="replacement-node">
            <button
              class="replacement-open map-hotspot"
              :class="{ chosen: selected === 2 }"
              data-stage="2"
              :aria-pressed="selected === 2"
              @click="selected = 2"
            >
              <Icon name="server" /><strong>Node B</strong
              ><span class="partition-chip">P2 + SlateDB</span
              ><small
                >Open from durable state.<br />Fence the previous writer.</small
              >
            </button>
            <button
              class="ready-record map-hotspot"
              :class="{ chosen: selected === 3 }"
              data-stage="3"
              :aria-pressed="selected === 3"
              @click="selected = 3"
            >
              <strong>READY</strong
              ><small
                >Recheck activation.<br />Publish the exact reservation.</small
              >
            </button>
          </div>
        </div>
        <button
          class="route-refresh map-hotspot"
          :class="{ chosen: selected === 4 }"
          data-stage="4"
          :aria-pressed="selected === 4"
          @click="selected = 4"
        >
          <Icon name="route" /><span
            ><strong>The service address stays the same</strong
            ><small
              >Refresh stale routes. Retry with the same operation
              identity.</small
            ></span
          >
        </button>
        <div class="flow-stem" aria-hidden="true">↓</div>
        <button
          class="recovery-receipt map-hotspot"
          :class="{ chosen: selected === 5 }"
          data-stage="5"
          :aria-pressed="selected === 5"
          @click="selected = 5"
        >
          <Icon name="journal" /><span
            ><strong>Verify the recovered state</strong
            ><small>Saved histories · results · visibility</small
            ><b>Full recovery acceptance remains open.</b></span
          >
        </button>
      </div>
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
