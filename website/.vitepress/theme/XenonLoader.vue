<script setup lang="ts">
import { withBase } from 'vitepress';
withDefaults(defineProps<{ size?: number; label?: string }>(), {
  size: 44,
  label: 'Loading',
});
</script>

<template>
  <span class="xenon-loader" role="status">
    <img :src="withBase('/brand/loader.svg')" :width="size" :height="size" alt="" />
    <span class="xenon-loader-label">{{ label }}</span>
  </span>
</template>

<style scoped>
.xenon-loader { display: inline-flex; align-items: center; }
.xenon-loader img { display: block; border-radius: 8px; }
.xenon-loader-label { position: absolute; width: 1px; height: 1px; padding: 0; overflow: hidden; clip-path: inset(50%); white-space: nowrap; }
</style>
