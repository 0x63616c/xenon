<script setup>
import { withBase } from "vitepress";
import Icon from "./ArchitectureIcon.vue";
defineProps({
  interactive: Boolean,
  compact: Boolean,
  selected: { type: Number, default: -1 },
});
const emit = defineEmits(["select"]);
</script>
<template>
  <div
    class="system-map"
    aria-label="Temporal persistence through a Xenon node pool into S3-compatible object storage"
  >
    <component
      v-if="!compact"
      :is="interactive ? 'button' : 'div'"
      class="map-clients map-hotspot"
      :class="{ chosen: selected === 0 }"
      :data-stage="interactive ? 0 : undefined"
      :aria-pressed="interactive ? selected === 0 : undefined"
      @click="emit('select', 0)"
    >
      <span><Icon name="code" />Your application</span>
      <span><Icon name="pulse" />Omes</span>
      <span><Icon name="window" />Temporal UI</span>
    </component>
    <div v-if="!compact" class="map-connection">
      <span>Temporal APIs</span><i></i>
    </div>
    <component
      :is="interactive ? 'button' : 'div'"
      class="temporal-engine map-hotspot"
      :class="{ chosen: selected === 1 }"
      :data-stage="interactive ? 1 : undefined"
      :aria-pressed="interactive ? selected === 1 : undefined"
      @click="emit('select', 1)"
    >
      <span class="engine-heading"
        ><Icon name="workflow" /><span
          ><strong>Temporal Server</strong
          ><small>The workflow engine · one or more instances</small></span
        ></span
      >
      <span v-if="!compact" class="temporal-services"
        ><span>Frontend</span><span>History</span><span>Matching</span
        ><span>Worker</span></span
      >
      <span class="adapter-strip"
        >Xenon persistence adapters <span>execution + visibility</span></span
      >
    </component>
    <div class="map-connection">
      <span>Complete persistence operations</span><i></i>
    </div>
    <div class="xenon-pool">
      <div class="pool-heading">
        <img :src="withBase('/brand/mark.svg')" alt="" /><strong>Xenon</strong
        ><span>Storage node pool</span>
      </div>
      <component
        :is="interactive ? 'button' : 'div'"
        class="service-address map-hotspot"
        :class="{ chosen: selected === 2 }"
        :data-stage="interactive ? 2 : undefined"
        :aria-pressed="interactive ? selected === 2 : undefined"
        @click="emit('select', 2)"
      >
        <Icon name="route" /><span
          ><strong>One service address</strong
          ><small>Routes to any ready node</small></span
        >
      </component>
      <div class="pool-fanout" aria-hidden="true"><i></i><i></i><i></i></div>
      <div class="node-rack">
        <div
          v-for="(node, i) in ['A', 'B', 'C']"
          :key="node"
          class="compute-node"
        >
          <component
            :is="interactive ? 'button' : 'div'"
            class="node-face map-hotspot"
            :class="{ chosen: selected === 3 }"
            :data-stage="interactive ? 3 : undefined"
            :aria-pressed="interactive ? selected === 3 : undefined"
            @click="emit('select', 3)"
          >
            <Icon name="server" /><strong>Node {{ node }}</strong
            ><small>Go process</small>
          </component>
          <component
            :is="interactive ? 'button' : 'div'"
            class="partition-slot map-hotspot"
            :class="{ chosen: selected === 4 }"
            :data-stage="interactive ? 4 : undefined"
            :aria-pressed="interactive ? selected === 4 : undefined"
            @click="emit('select', 4)"
          >
            <span class="partition-tag">P{{ i + 1 }} <span>owner</span></span>
            <Icon name="layers" /><strong>SlateDB</strong
            ><small>Inside this node</small>
          </component>
          <span class="rack-vents" aria-hidden="true">▰ ▰ ▰ ▰ ▰</span>
        </div>
      </div>
      <div class="forwarding-caption">
        <span aria-hidden="true">⇄</span
        ><span
          >Receiving node ≠ owner?<br class="mobile-break" />
          Forward internally.</span
        >
      </div>
      <p class="placement-caption">
        Illustrative placement. Each node may own several partitions; each
        partition has one writer.
      </p>
    </div>
    <div class="map-connection durable-connection">
      <span>SlateDB writes directly to object storage</span><i></i>
    </div>
    <component
      :is="interactive ? 'button' : 'div'"
      class="object-store map-hotspot"
      :class="{ chosen: selected === 5 }"
      :data-stage="interactive ? 5 : undefined"
      :aria-pressed="interactive ? selected === 5 : undefined"
      @click="emit('select', 5)"
    >
      <Icon name="bucket" /><span class="storage-description"
        ><strong>S3-compatible object storage</strong
        ><small>The durable layer · local node disks are disposable</small
        ><span class="object-groups"
          ><span>Application data</span><span>Ownership</span
          ><span>Topology</span></span
        ></span
      >
    </component>
    <div class="provider-options">
      <span class="provider-label">CHOOSE YOUR OBJECT STORE</span>
      <div class="provider-fanout" aria-hidden="true">
        <i></i><i></i><i></i>
      </div>
      <div class="provider-row">
        <span><strong>Amazon S3</strong><small>Deployment target</small></span
        ><span><strong>MinIO</strong><small>Used in local proofs</small></span
        ><span
          ><strong>Other S3 APIs</strong
          ><small>Validate per provider</small></span
        >
      </div>
    </div>
  </div>
</template>
