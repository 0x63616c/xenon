import { defineConfig } from "vitepress";
const base = process.env.SITE_BASE || "/";
export default defineConfig({
  title: "Xenon",
  description: "Temporal persistence, built on object storage.",
  srcDir: ".content",
  outDir: "dist",
  base,
  cleanUrls: false,
  appearance: false,
  head: [
    [
      "link",
      { rel: "icon", type: "image/svg+xml", href: `${base}brand/favicon.svg` },
    ],
    ["link", { rel: "alternate icon", href: `${base}brand/favicon.ico` }],
    [
      "link",
      { rel: "apple-touch-icon", href: `${base}brand/apple-touch-icon.png` },
    ],
    ["meta", { name: "theme-color", content: "#ffffff" }],
    ["meta", { name: "robots", content: "noindex,nofollow" }],
  ],
  themeConfig: {
    logo: { src: "/brand/lockup.svg", alt: "Xenon" },
    siteTitle: false,
    nav: [
      { text: "Architecture", link: "/docs/architecture" },
      { text: "Documentation", link: "/docs/" },
      { text: "Cloud", link: "/cloud" },
    ],
    sidebar: [
      {
        text: "Understand Xenon",
        items: [
          { text: "Overview", link: "/docs/" },
          { text: "Architecture", link: "/docs/architecture" },
          { text: "Verification status", link: "/docs/status" },
        ],
      },
      {
        text: "Build with Xenon",
        items: [
          { text: "Local development", link: "/docs/development" },
          { text: "Source code tour", link: "/docs/code-tour" },
          { text: "Testing & evidence", link: "/docs/testing" },
          { text: "Temporal upgrades", link: "/docs/upgrades" },
          { text: "Operations & recovery", link: "/docs/operations" },
        ],
      },
    ],
    search: { provider: "local" },
    outline: [2, 3],
    footer: {
      message: `<img class="footer-logo" src="${base}brand/lockup.svg" alt="Xenon" width="112" height="32">Private development preview · Proprietary software`,
      copyright: "Xenon · Cloud coming soon",
    },
  },
});
