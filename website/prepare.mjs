import { cpSync, mkdirSync, rmSync, readFileSync, writeFileSync } from "node:fs";
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
// Explicitly publish only the reviewed branding assets and standalone editor.
cpSync(new URL("../assets/brand/", import.meta.url), new URL("./.content/public/brand/", import.meta.url), { recursive: true });
mkdirSync(new URL("./.content/public/brand/editor/", import.meta.url), { recursive: true });
cpSync(new URL("../tools/logo-editor/index.html", import.meta.url), new URL("./.content/public/brand/editor/index.html", import.meta.url));
mkdirSync(new URL("./.content/public/fonts/", import.meta.url), { recursive: true });
cpSync(new URL("../assets/fonts/PlusJakartaSans.ttf", import.meta.url), new URL("./.content/public/fonts/PlusJakartaSans.ttf", import.meta.url));
cpSync(new URL("../assets/fonts/VarelaRound-Regular.ttf", import.meta.url), new URL("./.content/public/fonts/VarelaRound-Regular.ttf", import.meta.url));
const gallery = new URL("./.content/public/brand/spinners/index.html", import.meta.url);
writeFileSync(gallery, readFileSync(gallery, "utf8").replace("../../../tools/logo-editor/index.html", "../editor/index.html"));
