<script setup>
import { withBase } from "vitepress";
</script>
<template>
  <main class="x-home">
    <aside
      class="construction-banner"
      aria-label="Xenon is in active development"
    >
      <span aria-hidden="true">⚠</span>
      <strong>UNDER CONSTRUCTION</strong>
      <span>Experimental software — follow the build as it takes shape.</span>
      <span aria-hidden="true">⚠</span>
    </aside>
    <section class="hero">
      <div class="eyebrow"><span class="status-dot"></span> IN DEVELOPMENT</div>
      <h1>Temporal,<br /><span>backed by S3.</span></h1>
      <p class="hero-copy">
        Xenon brings object storage to Temporal. Keep the workflow engine, SDKs
        and UI you know, with durable state stored in S3.
      </p>
      <div class="actions">
        <a class="primary" :href="withBase('/docs/architecture.html')"
          >Explore the architecture <span>↗</span></a
        ><a class="text-link" :href="withBase('/docs/')"
          >Read the docs <span>→</span></a
        >
      </div>
      <nav class="start-here" aria-label="Start here">
        <a :href="withBase('/docs/')">
          <span>01</span><strong>Understand</strong
          ><small>Architecture and constraints</small>
        </a>
        <a :href="withBase('/docs/status.html')">
          <span>02</span><strong>Verify</strong
          ><small>Evidence and open gates</small>
        </a>
        <a href="https://github.com/0x63616c/xenon">
          <span>03</span><strong>Follow</strong
          ><small>Source and roadmap</small>
        </a>
      </nav>
      <div
        class="hero-system"
        aria-label="Temporal connects through one Xenon endpoint to identical Xenon agents, with durable state in S3"
      >
        <div class="system-top">
          <span class="small-icon">T</span><strong>Temporal</strong
          ><span>SDKs · Omes · UI</span>
        </div>
        <div class="connector vertical"></div>
        <div class="system-service">
          <img :src="withBase('/brand/spinners/counter-white.svg')" alt="" />
          <div>
            <strong>One Xenon endpoint</strong
            ><span class="service-copy"
              >One ingress for every client transport</span
            >
          </div>
          <span class="live-dot"></span>
        </div>
        <div class="branch"><i></i><i></i><i></i></div>
        <div class="system-nodes">
          <div v-for="(n, i) in ['A', 'B', 'C']" :key="n" class="system-node">
            <span class="node-caption">Xenon Agent {{ n }}</span>
            <div class="partition-bars">
              <i
                v-for="j in 3"
                :key="j"
                :class="{ blue: (j + i) % 3 === 0, mint: (j + i) % 2 === 0 }"
              ></i>
            </div>
            <span class="node-engine">Temporal + Xenon storage runtime</span>
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
        <span class="eyebrow">BUILT FOR TEMPORAL</span>
        <h2>Your workflows.<br />A new foundation.</h2>
      </div>
      <div class="principle-grid">
        <article>
          <span class="number">01</span>
          <h3>Keep building with Temporal.</h3>
          <p>
            Write workflows with the Temporal SDKs. Xenon connects beneath
            Temporal Server to store execution state and make workflows
            searchable in the Temporal UI.
          </p>
        </article>
        <article>
          <span class="number">02</span>
          <h3>Store state in S3.</h3>
          <p>
            Xenon stores application data through SlateDB and keeps ownership
            metadata in S3. Local disks are disposable, so recovery starts from
            object storage.
          </p>
        </article>
        <article>
          <span class="number">03</span>
          <h3>Connect through one endpoint.</h3>
          <p>
            Temporal connects to one Xenon service address. Each ready node
            handles a request locally or forwards it to the node that owns the
            data.
          </p>
        </article>
      </div>
    </section>
    <section class="architecture-teaser section-width">
      <div>
        <span class="eyebrow">ARCHITECTURE</span>
        <h2>Follow a write.<br />All the way to S3.</h2>
        <p>
          See how Xenon routes a request, saves the result, and waits for
          durable storage before replying. Then explore what happens when a node
          fails.
        </p>
        <a class="text-link" :href="withBase('/docs/architecture.html')"
          >Follow a request <span>→</span></a
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
        <span class="eyebrow">ENGINEERING</span>
        <h2>Kill a node.<br />Check what survives.</h2>
      </div>
      <div>
        <p>
          A passing three-agent component run moved live work, restarted a whole
          agent, discarded every local cache, and recovered matching histories.
          Full Omes and real AWS S3 acceptance remain open.
        </p>
        <a class="text-link" :href="withBase('/docs/status.html')"
          >Read the test results <span>→</span></a
        >
      </div>
    </section>
    <section class="cloud-section">
      <span class="eyebrow">XENON CLOUD™</span>
      <h2>Coming soon.</h2>
      <a class="text-link" :href="withBase('/cloud.html')"
        >Discover Xenon Cloud <span>→</span></a
      >
    </section>
  </main>
</template>
