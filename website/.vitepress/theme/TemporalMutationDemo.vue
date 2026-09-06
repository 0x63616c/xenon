<script setup>
import { computed, ref } from "vue";
const selected = ref(0);
const steps = [
  {
    label: "1. Assemble",
    title: "Close mutable state into one mutation",
    copy: "The History service turns its in-memory changes into a WorkflowMutation: execution state, changed maps, event batches, task slices, and optimistic conditions travel together.",
    records: [
      "history events",
      "mutable-state delta",
      "generated tasks",
      "record condition",
    ],
    code: "CloseTransactionAsMutation",
    location: "mutable_state_impl.go:6914",
  },
  {
    label: "2. Persist",
    title: "Give persistence one complete update",
    copy: "The transaction layer passes the mutation and event batches to the shard. The persistence manager serializes them before calling the configured execution store.",
    records: [
      "serialized events",
      "serialized mutation",
      "range ID",
      "update mode",
    ],
    code: "UpdateWorkflowExecution",
    location: "execution_manager.go:132",
  },
  {
    label: "3. Wake",
    title: "Notify only after an update may have committed",
    copy: "After the call, Temporal notifies the engine when the result is success or possibly succeeded. The durable task rows remain the source of work; notification merely wakes the right processor.",
    records: [
      "durable task rows",
      "processor notification",
      "high watermark",
      "reader slice",
    ],
    code: "NotifyNewTasks",
    location: "transaction_impl.go:601",
  },
  {
    label: "4. Execute",
    title: "Readers load; executables hand work to an executor",
    copy: "A queue reader selects persisted tasks and submits them. An executable then invokes its category-specific executor, which may create another workflow update.",
    records: [
      "selected task batch",
      "submitted executable",
      "category executor",
      "next mutation",
    ],
    code: "loadAndSubmitTasks",
    location: "reader.go:426",
  },
];
const step = computed(() => steps[selected.value]);
const advance = () => (selected.value = (selected.value + 1) % steps.length);
</script>
<template>
  <section
    class="temporal-demo"
    aria-label="Interactive Temporal mutation path"
  >
    <div class="temporal-demo-top">
      <div>
        <span class="eyebrow">INTERACTIVE CODE PATH</span>
        <h2>{{ step.title }}</h2>
      </div>
      <button class="temporal-next" type="button" @click="advance">
        Next stage <span>→</span>
      </button>
    </div>
    <div class="temporal-stages" role="tablist" aria-label="Mutation stages">
      <button
        v-for="(item, index) in steps"
        :key="item.label"
        type="button"
        role="tab"
        :aria-selected="selected === index"
        :class="{ active: selected === index }"
        @click="selected = index"
      >
        {{ item.label }}
      </button>
    </div>
    <div class="temporal-flow">
      <div class="temporal-card temporal-origin">
        <span>History service</span><b>Workflow change</b
        ><small>event + state transition</small>
      </div>
      <div class="temporal-arrow" aria-hidden="true">→</div>
      <div class="temporal-card temporal-payload">
        <span>Mutation envelope</span><b>{{ step.code }}</b
        ><small>{{ step.location }}</small>
      </div>
      <div class="temporal-arrow" aria-hidden="true">→</div>
      <div class="temporal-card temporal-destination">
        <span v-if="selected < 2">Persistence</span
        ><span v-else>History queue</span
        ><b v-if="selected < 2">Execution store</b
        ><b v-else-if="selected === 2">Wake reader</b><b v-else>Run executor</b
        ><small v-if="selected < 2">history, state, task records</small
        ><small v-else>persisted task is authoritative</small>
      </div>
    </div>
    <div class="temporal-explainer">
      <p>{{ step.copy }}</p>
      <ul aria-label="Data carried or acted upon">
        <li v-for="record in step.records" :key="record">{{ record }}</li>
      </ul>
    </div>
    <p class="temporal-caption">
      Illustrative reading aid for the linked v1.31.2 source; it is not an
      execution trace or a Xenon compatibility result.
    </p>
  </section>
</template>
