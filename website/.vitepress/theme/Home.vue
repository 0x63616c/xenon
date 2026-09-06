<script setup>
import { withBase } from "vitepress";
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
        Your workflows stay Temporal.<br />Your durable state lives in S3.
      </p>
      <div class="actions">
        <a class="primary" :href="withBase('/docs/architecture.html')"
          >Explore the architecture <span>↗</span></a
        ><a class="text-link" :href="withBase('/docs/')"
          >Read the docs <span>→</span></a
        >
      </div>
      <div
        class="hero-system"
        aria-label="Temporal connects through one Xenon endpoint to Go nodes, with durable state in S3"
      >
        <div class="system-top">
          <span class="small-icon">T</span><strong>Temporal</strong
          ><span>SDKs · UI · workflows</span>
        </div>
        <div class="connector vertical"></div>
        <div class="system-service">
          <img :src="withBase('/brand/mark-white.svg')" alt="" /><strong
            >One Xenon endpoint</strong
          ><span class="live-dot"></span>
        </div>
        <div class="branch"><i></i><i></i><i></i></div>
        <div class="system-nodes">
          <div v-for="(n, i) in ['A', 'B', 'C']" :key="n" class="system-node">
            <span class="node-caption">GO NODE {{ n }}</span>
            <div class="partition-bars">
              <i
                v-for="j in 3"
                :key="j"
                :class="{ active: (j + i) % 3 === 0 }"
              ></i>
            </div>
            <span class="node-engine">SlateDB</span>
          </div>
        </div>
        <div class="storage-line"></div>
        <div class="system-storage">
          <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <ellipse cx="12" cy="5" rx="8" ry="3" />
            <path d="M4 5v14c0 4 16 4 16 0V5M4 12c0 4 16 4 16 0" /></svg
          ><strong>S3</strong><span>Application data + ownership metadata</span>
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
          <h3>Durability belongs in S3.</h3>
          <p>
            Go nodes embed SlateDB. Application records and ownership metadata
            live in object storage; local disks are disposable.
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
          Follow a request from the SDK to S3. Explore partition admission,
          durable replay, and what happens when an owner disappears.
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
