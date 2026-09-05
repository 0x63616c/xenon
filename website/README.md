# Xenon site

VitePress renders the curated `docs/guide/` tree. `prepare.mjs` copies only that
content into ignored `.content/`. The static output is `website/dist/`; the repo,
private source files, runtime evidence and `.local/` are never included.

```sh
npm ci --prefix website
npm run dev --prefix website
npm run build --prefix website
npm run preview --prefix website
```

The preview binds loopback port 4178. Use Node 24.19.0 for parity with CI. The locked
Vite 6.4.3 override removes the development-server advisories in VitePress 1.6.4's
older transitive Vite version; production build and browser checks exercise that
combination. `npm audit` reported zero vulnerabilities at initial validation.

For the committed browser regression, install the pinned Chromium first:

```sh
cd website
npx playwright install chromium
npm run test:ui -- http://localhost:4178/ ../.local/site-qa
```

The test uses new isolated browser contexts at 1440×1000, 820×1180 and 390×844. It checks
eight pages, overflow, script/network errors and the three interactive architecture
views, and retains full-page screenshots. This is website QA, not Xenon runtime
acceptance. Review the screenshots in addition to checking the JSON receipt.

For a GitHub Pages project path, build and preview with `SITE_BASE=/xenon/` and pass
`http://localhost:4178/xenon/` to the browser test. The workflow uploads only static
assets from `website/dist`. Deploy is disabled unless a manual dispatch explicitly
sets `deploy` and the separately approved repository variable
`XENON_SITE_DEPLOYMENT_APPROVED` equals `true`. It has not been enabled. Configure
Pages and any environment approval policy only after publication is authorized;
a private repository does not itself guarantee a private Pages website.

No public deployment, source release or Cloud availability is authorized by this
branch. Hosted CI billing remains an external gate, not a locally waived check.
