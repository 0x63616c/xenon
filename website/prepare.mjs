import { cpSync, mkdirSync, rmSync } from "node:fs";
// Only explicitly authored guide content enters the site; never copy repo/evidence.
rmSync(new URL("./.content/", import.meta.url), {
  recursive: true,
  force: true,
});
mkdirSync(new URL("./.content/", import.meta.url), { recursive: true });
cpSync(
  new URL("../docs/guide/", import.meta.url),
  new URL("./.content/", import.meta.url),
  { recursive: true },
);

// Publish the shared artwork, keeping the legacy symbol URL working.
cpSync(
  new URL("../assets/brand/", import.meta.url),
  new URL("./.content/public/brand/", import.meta.url),
  { recursive: true },
);
cpSync(
  new URL("../assets/brand/mark.svg", import.meta.url),
  new URL("./.content/public/xenon.svg", import.meta.url),
);
