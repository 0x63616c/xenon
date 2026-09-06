# Branding and interactive website QA

Validated source: `876eff21d754696d55c95b6b3ffa8f54784f5d4b` (clean checkout).
The [machine receipt](result.json) records the source, locked dependency hash,
static build hashes, Node v24.19.0 and Chromium 141.0.7390.37.

All 30 page/viewport combinations passed (ten pages at 1440×1000, 820×1180 and
390×844), with no browser or HTTP errors and no horizontal overflow. Checks
cover actual Space Grotesk loading, branding images and icons under `/xenon/`,
homepage role selection, all six selectable roles in each of three architecture
views, keyboard tab navigation, local search and mobile navigation.

## Reproduce

With Node 24.19.0, from the tested checkout:

```sh
npm ci --prefix website
npm run format:check --prefix website
SITE_BASE=/xenon/ npm run build --prefix website
cd website
npx playwright install chromium
SITE_BASE=/xenon/ node node_modules/vitepress/bin/vitepress.js preview --host 127.0.0.1 --port 4192
```

In another terminal, from `website/`:

```sh
npm run test:ui -- http://127.0.0.1:4192/xenon/ ../.local/site-qa
```

Stop the preview with Ctrl-C. The test creates isolated browser contexts and
closes them on completion. Reports and screenshots go to the specified ignored
local directory. A dirty run is marked `development-passed`, never proof-passed.

## Visual review

- [Desktop branding](desktop-hero.png) and [mobile branding](mobile-hero.png).
- [Desktop request path](desktop-diagram-request-path.png) and [mobile request path](mobile-diagram-request-path.png).
- [Inside a node](mobile-diagram-inside-a-node.png) and [ownership movement](mobile-diagram-move-recover.png).

Reviewed the logo and font, diagram nesting, distinct engine/service/compute/
storage roles, legible mobile labels and provider alternatives. Diagram-only
captures temporarily hide fixed site navigation to avoid crossing the captured
panel; full-page/hero captures retain the normal interface. These screenshots
are QA evidence; the product uses interactive Vue/HTML with SVG only for icons.

## Scope and limits

This proves the static site's rendering and interactions, not Xenon runtime
correctness, backend-provider acceptance or a running cluster. The site is a
private local preview; deployment remains disabled. GitHub Actions jobs are
blocked by the existing account billing restriction. The logo/README assets
were separately merged to main in PR #82; site integration is PR #83.
