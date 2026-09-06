import { defineConfig } from "vitepress";
const siteBase = process.env.SITE_BASE || "/";
export default defineConfig({
  title: "Xenon",
  description:
    "Xenon brings S3-backed persistence to Temporal. Explore the architecture, recovery tests and developer documentation. Currently in development.",
  srcDir: ".content",
  outDir: "dist",
  base: siteBase,
  cleanUrls: false,
  appearance: false,
  head: [
    ["meta", { name: "theme-color", content: "#f5c518" }],
    ["link", { rel: "icon", type: "image/svg+xml", href: `${siteBase}brand/favicon.svg` }],
    ["link", { rel: "apple-touch-icon", href: `${siteBase}brand/apple-touch-icon.png` }],
  ],
  themeConfig: {
    logo: "/brand/logo.svg",
    siteTitle: "Xenon",
    nav: [
      { text: "Architecture", link: "/docs/architecture" },
      { text: "Documentation", link: "/docs/" },
      { text: "Blog", link: "/blog/" },
      { text: "Cloud", link: "/cloud" },
      { text: "GitHub ↗", link: "https://github.com/0x63616c/xenon" },
    ],
    sidebar: [
      {
        text: "Understand Xenon",
        items: [
          { text: "Overview", link: "/docs/" },
          { text: "Architecture", link: "/docs/architecture" },
          { text: "Verification status", link: "/docs/status" },
          { text: "Brand and motion", link: "/brand" },
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
  },
});
