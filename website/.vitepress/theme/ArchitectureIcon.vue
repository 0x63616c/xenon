<script setup>
defineProps({ name: String });
const paths = {
  workflow: "M4 5h6v6H4zM14 13h6v6h-6zM7 11v5h7M10 8h7v5",
  code: "m8 6-6 6 6 6m8-12 6 6-6 6m-3-14-2 16",
  pulse: "M2 12h5l3-7 4 14 3-7h5",
  window: "M3 4h18v16H3zM3 9h18M7 6.5h.01M10 6.5h.01",
  route: "M12 3v6M4 21v-7h16v7M12 9v12M8 6l4 4 4-4",
  server: "M3 3h18v7H3zM3 14h18v7H3zM6 6.5h.01M6 17.5h.01M11 6.5h6M11 17.5h6",
  layers: "m12 3 10 5-10 5L2 8zm-10 9 10 5 10-5M2 16l10 5 10-5",
  bucket:
    "M3 6c0-4 18-4 18 0s-18 4-18 0Zm0 0 2 13c1 3 13 3 14 0l2-13M8 13c2 1 6 1 8 0",
  packet: "M2 5h20v14H2zm0 0 10 8L22 5",
  gate: "M4 3v18M20 3v18M4 8h16M8 8v8M12 8v8M16 8v8",
  journal: "M6 3h15v18H6zM3 6h5M3 11h5M3 16h5M11 8h6M11 12h6M11 16h4",
  check: "m4 12 5 5L20 6",
  shield: "m12 2 9 4v6c0 6-9 10-9 10S3 18 3 12V6zm-4 10 3 3 5-6",
  terminal: "M2 4h20v16H2zm4 5 3 3-3 3m7 0h5",
  document: "M5 2h9l5 5v15H5zm9 0v6h5M9 12h6M9 16h6",
};
</script>
<template>
  <svg
    class="architecture-icon"
    viewBox="0 0 24 24"
    fill="none"
    aria-hidden="true"
  >
    <path
      :d="paths[name] || paths.layers"
      stroke="currentColor"
      stroke-width="1.4"
      stroke-linecap="round"
      stroke-linejoin="round"
    />
  </svg>
</template>
