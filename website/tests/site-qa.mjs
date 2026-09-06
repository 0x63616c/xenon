import { chromium } from "playwright";
import { mkdir, writeFile, readFile, readdir } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
const base = process.argv[2] || "http://localhost:4178/";
const output = process.argv[3] || "../.local/site-qa";
await mkdir(output, { recursive: true });
const report = {
  scope: "static website QA only",
  base,
  node_version: process.version,
  source: execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim(),
  dirty: !!execFileSync("git", ["status", "--porcelain"], {
    encoding: "utf8",
  }).trim(),
  package_lock_sha256: createHash("sha256")
    .update(await readFile("package-lock.json"))
    .digest("hex"),
  viewports: [],
  errors: [],
};
const browser = await chromium.launch({ headless: true });
report.browser_version = browser.version();
report.build_sha256 = {};
for (const file of (await readdir("dist", { recursive: true })).sort()) {
  if (!file.includes(".")) continue;
  try {
    report.build_sha256[file] = createHash("sha256")
      .update(await readFile("dist/" + file))
      .digest("hex");
  } catch (error) {
    if (error.code !== "EISDIR") throw error;
  }
}
let currentPage;
try {
  for (const [name, width, height] of [
    ["desktop", 1440, 1000],
    ["tablet", 820, 1180],
    ["mobile", 390, 844],
  ]) {
    const context = await browser.newContext({
      viewport: { width, height },
      deviceScaleFactor: 1,
    });
    const page = await context.newPage();
    currentPage = page;
    page.on("pageerror", (e) => report.errors.push(String(e)));
    page.on("response", (r) => {
      if (r.status() >= 400) report.errors.push(`${r.status()} ${r.url()}`);
    });
    const checked = [];
    for (const path of [
      "",
      "docs/architecture.html",
      "docs/",
      "docs/status.html",
      "docs/development.html",
      "docs/code-tour.html",
      "docs/testing.html",
      "docs/upgrades.html",
      "docs/operations.html",
      "cloud.html",
    ]) {
      await page.goto(new URL(path, base).href, { waitUntil: "networkidle" });
      if (!(await page.locator("h1").count()))
        throw new Error(`missing title ${name}/${path}`);
      if (
        await page.evaluate(
          () => document.documentElement.scrollWidth > innerWidth + 1,
        )
      )
        throw new Error(`horizontal overflow ${name}/${path}`);
      await page.evaluate(() => document.fonts.ready);
      const typography = await page.evaluate(() => ({
        loaded: [...document.fonts].some(
          (font) =>
            font.family.replaceAll('"', "") === "Space Grotesk" &&
            font.status === "loaded",
        ),
        heading: getComputedStyle(document.querySelector("h1")).fontFamily,
        body: getComputedStyle(document.body).fontFamily,
      }));
      if (
        !typography.loaded ||
        !typography.heading.includes("Space Grotesk") ||
        !typography.body.includes("Space Grotesk")
      )
        throw new Error(
          `brand font not loaded ${name}/${path}: ${JSON.stringify(typography)}`,
        );
      const brokenImages = await page
        .locator("img")
        .evaluateAll((images) =>
          images
            .filter((image) => !image.complete || image.naturalWidth === 0)
            .map((image) => image.src),
        );
      if (brokenImages.length)
        throw new Error(
          `broken images ${name}/${path}: ${brokenImages.join(", ")}`,
        );
      const logo = page.locator(".VPNavBarTitle img");
      if (!(await logo.getAttribute("src"))?.endsWith("/brand/lockup.svg"))
        throw new Error(`missing brand lockup ${name}/${path}`);
      if (!path) {
        for (const rel of ["icon", "alternate icon", "apple-touch-icon"]) {
          const href = await page
            .locator(`link[rel="${rel}"]`)
            .getAttribute("href");
          const url = new URL(href, page.url());
          if (!url.href.startsWith(new URL("brand/", base).href))
            throw new Error(`icon escaped site base: ${url}`);
          const response = await context.request.get(url.href);
          if (!response.ok() || !(await response.body()).length)
            throw new Error(`icon failed: ${url}`);
        }
      }
      if (!path) {
        await page.locator('.hero-system [data-stage="2"]').click();
        if (
          !(await page.locator(".hero-map-detail").innerText()).includes(
            "shared address",
          )
        )
          throw new Error(`homepage role explanation failed: ${name}`);
      }
      if (path === "docs/architecture.html") {
        const firstTab = page.getByRole("tab", { name: "Request path" });
        await firstTab.focus();
        await page.keyboard.press("ArrowRight");
        if (
          (await page
            .getByRole("tab", { name: "Inside a node" })
            .getAttribute("aria-selected")) !== "true"
        )
          throw new Error("keyboard tab navigation failed");
        for (const label of [
          "Inside a node",
          "Move & recover",
          "Request path",
        ]) {
          await page.getByRole("tab", { name: label }).click();
          const hotspots = page.locator(".architecture-explorer [data-stage]");
          const stages = await hotspots.evaluateAll((buttons) =>
            [...new Set(buttons.map((button) => button.dataset.stage))].sort(),
          );
          if (stages.join(",") !== "0,1,2,3,4,5")
            throw new Error(`architecture roles missing: ${label}`);
          for (const stage of stages) {
            const button = page
              .locator(`.architecture-explorer [data-stage="${stage}"]`)
              .first();
            await button.click();
            if ((await button.getAttribute("aria-pressed")) !== "true")
              throw new Error(`role selection failed: ${label}/${stage}`);
            if (!(await page.locator(".arch-inspector h3").innerText()).trim())
              throw new Error(`role explanation missing: ${label}/${stage}`);
          }
          await page.locator(".architecture-explorer").screenshot({
            path: `${output}/${name}-diagram-${label.toLowerCase().replaceAll(/[^a-z]+/g, "-")}.png`,
            animations: "disabled",
          });
        }
      }
      if (!path || path === "docs/architecture.html")
        await page.evaluate(() => scrollTo({ top: 0, behavior: "instant" }));
      if (!path || path === "docs/architecture.html")
        await page.screenshot({
          path: `${output}/${name}-${path ? "architecture" : "home"}.png`,
          fullPage: true,
          animations: "disabled",
        });
      if (!path)
        await page.screenshot({
          path: `${output}/${name}-hero.png`,
          animations: "disabled",
        });
      if (!path && name === "desktop") {
        await page.getByRole("button", { name: /Search/ }).click();
        await page.locator("#localsearch-input").fill("recovery");
        await page.locator("#localsearch-list").waitFor({ state: "visible" });
        await page.keyboard.press("Escape");
      }
      if (!path && name === "mobile") {
        await page.getByRole("button", { name: "mobile navigation" }).click();
        await page
          .getByRole("link", { name: "Architecture", exact: true })
          .waitFor({ state: "visible" });
        await page.getByRole("button", { name: "mobile navigation" }).click();
      }
      checked.push(path || "index.html");
    }
    report.viewports.push({ name, width, height, checked });
    await context.close();
  }
  if (report.errors.length)
    throw new Error("browser errors: " + report.errors.join("; "));
  if (
    execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim() !==
    report.source
  )
    throw new Error("source changed during QA");
  if (
    !!execFileSync("git", ["status", "--porcelain"], {
      encoding: "utf8",
    }).trim() !== report.dirty
  )
    throw new Error("checkout state changed during QA");
  report.result = report.dirty ? "development-passed" : "passed";
  report.proof_pass = !report.dirty;
} catch (e) {
  report.result = "failed";
  report.proof_pass = false;
  report.error = String(e);
  if (currentPage && !currentPage.isClosed()) {
    await currentPage.screenshot({
      path: `${output}/failure.png`,
      fullPage: true,
      animations: "disabled",
    });
    await writeFile(`${output}/failure.html`, await currentPage.content());
  }
  throw e;
} finally {
  await writeFile(`${output}/result.json`, JSON.stringify(report, null, 2));
  await browser.close();
}
