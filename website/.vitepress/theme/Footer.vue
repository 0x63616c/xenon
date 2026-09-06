<script setup>
import { withBase } from "vitepress";
const groups = [
  {
    title: "Explore",
    links: [
      ["Architecture", "/docs/architecture.html"],
      ["Xenon Cloud™", "/cloud.html"],
      ["Blog", "/blog/"],
      ["Verification status", "/docs/status.html"],
    ],
  },
  {
    title: "Develop",
    links: [
      ["Documentation", "/docs/"],
      ["Local development", "/docs/development.html"],
      ["Source code tour", "/docs/code-tour.html"],
    ],
  },
  {
    title: "Operate",
    links: [
      ["Testing & evidence", "/docs/testing.html"],
      ["Operations & recovery", "/docs/operations.html"],
      ["Temporal upgrades", "/docs/upgrades.html"],
    ],
  },
];
</script>
<template>
  <footer class="x-footer" aria-label="Site footer">
    <div class="footer-main">
      <a class="footer-brand" :href="withBase('/')" aria-label="Xenon home"
        ><img :src="withBase('/xenon.svg')" alt="" /><span>Xenon</span></a
      >
      <nav
        v-for="group in groups"
        :key="group.title"
        :aria-label="group.title + ' footer links'"
      >
        <h2>{{ group.title }}</h2>
        <a
          v-for="[label, path] in group.links"
          :key="path"
          :href="withBase(path)"
          >{{ label }}</a
        >
      </nav>
    </div>
    <div class="footer-bottom">
      <span>© 2026 Xenon. All rights reserved.</span
      ><a :href="withBase('/docs/')">Temporal persistence. Object storage.</a>
    </div>
  </footer>
</template>
