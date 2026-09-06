<script setup>
import { ref } from "vue";
const requested = ref(false);
import { withBase } from "vitepress";
</script>
<template>
  <main class="x-cloud">
    <div class="cloud-orbit" aria-hidden="true">
      <i></i><i></i><i></i><img :src="withBase('/xenon.svg')" alt="" />
    </div>
    <span class="eyebrow">XENON CLOUD™</span>
    <h1>Coming soon.</h1>
    <form class="cloud-signup" @submit.prevent="requested = true">
      <label for="cloud-email">Stay in the loop.</label>
      <div>
        <input
          id="cloud-email"
          type="email"
          autocomplete="email"
          required
          placeholder="Your email address"
          aria-describedby="signup-state"
        /><button type="submit">
          Notify me <span aria-hidden="true">→</span>
        </button>
      </div>
      <p id="signup-state" role="status">
        {{
          requested
            ? "Email signup is coming soon. Your address has not been saved."
            : "Email signup coming soon."
        }}
      </p>
    </form>
    <a class="text-link" :href="withBase('/docs/architecture.html')"
      >Explore the foundation <span>→</span></a
    >
  </main>
</template>
