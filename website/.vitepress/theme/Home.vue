<script setup>
import { withBase } from "vitepress";
import SystemDiagram from "./SystemDiagram.vue";
import { ref } from "vue";
const selectedRole = ref(-1);
const roles = [
  [
    "Your existing clients",
    "Applications, Omes and the UI continue to use Temporal's APIs.",
  ],
  [
    "Temporal runs your workflows",
    "The workflow engine stays in place. Its persistence adapters send complete operations to Xenon.",
  ],
  [
    "A shared address, not another storage node",
    "The service routes a request to any ready Xenon node. The local proof uses HAProxy; Kubernetes Service is the intended deployment equivalent.",
  ],
  [
    "Each node can receive and forward",
    "A Go process executes work for partitions it owns. For another owner's partition, it forwards the operation internally.",
  ],
  [
    "SlateDB lives inside the owner",
    "Each owned partition has its own embedded SlateDB handle and admission gate. One node can own several partitions.",
  ],
  [
    "Durable state lives in object storage",
    "The current storage API is S3-compatible. Local proofs use MinIO; Amazon S3 and other providers require their own acceptance runs.",
  ],
];
</script>
<template>
  <main class="x-home">
    <section class="hero">
      <img
        class="hero-mark"
        :src="withBase('/brand/mark.svg')"
        alt="Xenon"
        width="80"
        height="80"
      />
      <div class="eyebrow"><span class="status-dot"></span> IN DEVELOPMENT</div>
      <h1>Temporal.<br /><span>Object storage.</span></h1>
      <p class="hero-copy">
        Your workflows stay Temporal.<br />Your durable state lives in object
        storage.
      </p>
      <div class="actions">
        <a class="primary" :href="withBase('/docs/architecture.html')"
          >Explore the architecture <span>↗</span></a
        ><a class="text-link" :href="withBase('/docs/')"
          >Read the docs <span>→</span></a
        >
      </div>
      <div class="hero-system">
        <p class="diagram-instruction">Select a component to see its role.</p>
        <SystemDiagram
          compact
          interactive
          :selected="selectedRole"
          @select="selectedRole = $event"
        />
        <div class="hero-map-detail" aria-live="polite">
          <template v-if="selectedRole >= 0"
            ><strong>{{ roles[selectedRole][0] }}</strong>
            <p>{{ roles[selectedRole][1] }}</p></template
          >
        </div>
      </div>
    </section>
    <section class="principles section-width">
      <div class="section-lead">
        <span class="eyebrow">FAMILIAR ENGINE. DIFFERENT FOUNDATION.</span>
        <h2>Keep the workflow.<br />Rethink the storage.</h2>
      </div>
      <div class="principle-grid">
        <article>
          <span class="number">01</span>
          <h3>Temporal stays Temporal.</h3>
          <p>
            Xenon connects at the persistence layer. The workflow engine,
            frontend, existing SDKs and UI retain their roles.
          </p>
        </article>
        <article>
          <span class="number">02</span>
          <h3>Built for object storage.</h3>
          <p>
            Go nodes embed SlateDB. Application records and ownership metadata
            use S3-compatible object storage; local disks are disposable.
          </p>
        </article>
        <article>
          <span class="number">03</span>
          <h3>One address. Many owners.</h3>
          <p>
            Every node accepts complete operations. It serves the partitions it
            owns and forwards the rest to their current owner.
          </p>
        </article>
      </div>
    </section>
    <section class="architecture-teaser section-width">
      <div>
        <span class="eyebrow">LOOK INSIDE</span>
        <h2>Simple to enter.<br />Careful at every commit.</h2>
        <p>
          Follow a request from the SDK to object storage. Explore partition
          admission, durable replay, and what happens when an owner disappears.
        </p>
        <a class="text-link" :href="withBase('/docs/architecture.html')"
          >Open the interactive guide <span>→</span></a
        >
      </div>
      <div class="commit-flow">
        <div><span>01</span> Check ownership</div>
        <div><span>02</span> Execute + record outcome</div>
        <div><span>03</span> Await durability</div>
        <div class="final"><span>04</span> Return the result <b>↗</b></div>
      </div>
    </section>
    <section class="status-section section-width">
      <div>
        <span class="eyebrow">BUILT WITH EVIDENCE</span>
        <h2>Work in progress.<br />Claims you can inspect.</h2>
      </div>
      <div>
        <p>
          Component proofs and a real multi-instance smoke test exercise
          recovery, Omes and the unchanged Temporal UI. The full acceptance gate
          is still open.
        </p>
        <a class="text-link" :href="withBase('/docs/status.html')"
          >See what is verified <span>→</span></a
        >
      </div>
    </section>
    <section class="cloud-section">
      <span class="eyebrow">XENON CLOUD</span>
      <h2>Coming soon.</h2>
      <p>A future managed offering. No service is available today.</p>
      <a class="text-link" :href="withBase('/cloud.html')"
        >About Xenon Cloud <span>→</span></a
      >
    </section>
  </main>
</template>
