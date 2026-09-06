<script setup>
import { withBase } from "vitepress";
const posts = [
  {
    category: "Architecture",
    title: "One address. Many owners.",
    copy: "How a request finds its partition—and why finding the owner is only the first step.",
    path: "one-address-many-owners",
    graphic: "routing",
  },
  {
    category: "Durability",
    title: "When a reply disappears.",
    copy: "A lost connection leaves a question. A durable outcome gives the retry an answer.",
    path: "when-a-reply-disappears",
    graphic: "durability",
  },
  {
    category: "Temporal internals",
    title: "One mutation, then a queue.",
    copy: "A source-guided tour of how History turns a workflow change into persisted state and follow-on work.",
    path: "one-mutation-then-a-queue",
    graphic: "temporal",
  },
];
</script>
<template>
  <main class="x-blog section-width">
    <header>
      <span class="eyebrow">THE XENON BLOG</span>
      <h1>From the inside.</h1>
      <p>Notes on the systems behind the workflows.</p>
    </header>
    <div class="blog-grid">
      <a
        v-for="post in posts"
        :key="post.path"
        class="blog-card"
        :href="withBase('/blog/' + post.path + '.html')"
      >
        <div class="blog-art" :class="post.graphic" aria-hidden="true">
          <template v-if="post.graphic === 'routing'"
            ><span>Request</span><i>↓</i>
            <div><b>A</b><b>B</b><b>C</b></div></template
          ><template v-else-if="post.graphic === 'temporal'"
            ><span>State change</span><i>↓</i
            ><span class="record"
              >Mutation<br /><small>Tasks included.</small></span
            ></template
          ><template v-else
            ><span>Commit</span><i>→</i
            ><span class="record"
              >Outcome<br /><small>Recorded.</small></span
            ></template
          >
        </div>
        <div class="blog-card-copy">
          <span class="eyebrow">{{ post.category }}</span>
          <h2>{{ post.title }}</h2>
          <p>{{ post.copy }}</p>
          <span class="text-link">Read the story <span>→</span></span>
        </div>
      </a>
    </div>
  </main>
</template>
