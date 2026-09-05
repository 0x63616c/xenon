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
      "architecture.html",
      "overview.html",
      "status.html",
      "development.html",
      "code-tour.html",
      "operations.html",
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
      if (path === "architecture.html") {
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
          const nodes = page.locator(".arch-node");
          if ((await nodes.count()) !== 6)
            throw new Error("architecture nodes missing");
          await nodes.last().click();
          await page
            .locator(".arch-inspector h3")
            .waitFor({ state: "visible" });
          if (!((await nodes.last().getAttribute("aria-pressed")) === "true"))
            throw new Error("selected node missing");
        }
      }
      if (!path || path === "architecture.html")
        await page.evaluate(() => scrollTo({ top: 0, behavior: "instant" }));
      if (!path || path === "architecture.html")
        await page.screenshot({
          path: `${output}/${name}-${path ? "architecture" : "home"}.png`,
          fullPage: true,
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
