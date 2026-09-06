<script setup>
import { ref, computed } from "vue";
const entry = ref("A"),
  owner = ref("B"),
  step = ref(0);
const steps = computed(() => [
  `The service delivers the request to node ${entry.value}.`,
  entry.value === owner.value
    ? `Node ${entry.value} owns this partition. No forwarding hop is needed.`
    : `Node ${entry.value} forwards the unchanged operation to owner ${owner.value}.`,
  `Node ${owner.value} checks fresh ownership inside the partition admission gate. A stale owner rejects the request.`,
  `After execution and durability checks, the result travels back to the caller.`,
]);
</script>
<template>
  <section class="routing-demo" aria-label="Illustrative request routing">
    <div class="routing-controls">
      <label
        >Entry node<select v-model="entry" @change="step = 0">
          <option>A</option>
          <option>B</option>
          <option>C</option>
        </select></label
      ><label
        >Partition owner<select v-model="owner" @change="step = 0">
          <option>A</option>
          <option>B</option>
          <option>C</option>
        </select></label
      >
    </div>
    <div class="routing-map" aria-hidden="true">
      <span>Temporal</span><i>→</i><b>{{ entry }}</b
      ><template v-if="entry !== owner"
        ><i>→</i><b>{{ owner }}</b></template
      ><i>→</i><span>S3</span>
    </div>
    <p aria-live="polite">{{ steps[step] }}</p>
    <div class="routing-bottom">
      <span>Step {{ step + 1 }} of 4</span
      ><button @click="step = (step + 1) % 4">
        {{ step === 3 ? "Start again" : "Next step" }} →
      </button>
    </div>
    <small
      >Illustrative request path. The executed proofs are linked below.</small
    >
  </section>
</template>
