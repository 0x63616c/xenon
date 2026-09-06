<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';
import { useRouter } from 'vitepress';
import XenonLoader from './XenonLoader.vue';

const router = useRouter();
const loading = ref(false);
let timer: ReturnType<typeof setTimeout> | undefined;
let restore: (() => void) | undefined;
function stop() {
  clearTimeout(timer);
  loading.value = false;
}
onMounted(() => {
  const before = router.onBeforePageLoad;
  const after = router.onAfterPageLoad;
  const start: NonNullable<typeof before> = async (to) => {
    stop();
    const result = await before?.(to);
    if (result !== false) {
      loading.value = true;
      // Do not leave a stuck status badge after a failed network navigation.
      timer = setTimeout(stop, 20000);
    }
    return result;
  };
  const finish: NonNullable<typeof after> = async (to) => {
    try { await after?.(to); } finally { stop(); }
  };
  router.onBeforePageLoad = start;
  router.onAfterPageLoad = finish;
  restore = () => {
    if (router.onBeforePageLoad === start) router.onBeforePageLoad = before;
    if (router.onAfterPageLoad === finish) router.onAfterPageLoad = after;
  };
});
onUnmounted(() => { stop(); restore?.(); });
</script>

<template>
  <div v-if="loading" class="xenon-page-loading">
    <XenonLoader :size="56" label="Loading page" />
  </div>
</template>

<style scoped>
.xenon-page-loading { position: fixed; right: 24px; bottom: 24px; z-index: 100; pointer-events: none; padding: 4px; background: #000; border: 1px solid #555; border-radius: 12px; box-shadow: 0 4px 20px #0003; }
</style>
