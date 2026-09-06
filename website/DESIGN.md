# Site polish references

Visually inspected in fresh Playwright contexts on 2026-09-06:

- https://www.apple.com/ — generous section spacing, compact navigation and a clear primary action.
- https://linear.app/ — precise type hierarchy and product-specific visual explanation.
- https://vercel.com/ — restrained borders, direct navigation and strong contrast.
- https://posthog.com/ — a distinctive product identity and substantial editorial/documentation navigation.
- https://basecamp.com/ — concrete, readable language and direct links to useful detail.

Xenon retains its own white/blue visual language and code-native diagrams. No borrowed imagery, customer claims, pricing or signup integration. The Cloud form validates locally, stores/sends nothing, and reports that signup is coming soon. The routing article interaction is an illustrative explanation, not a correctness proof.

Reproduce with the pinned Node installer, `SITE_BASE=/xenon/ npm run build`, a VitePress preview on port 4180, and `npm run test:ui -- http://127.0.0.1:4180/xenon/ ../.local/site-qa`. The test covers all 13 routes at desktop/tablet/mobile sizes, real footer destinations, Cloud validation/no persistence, navigation/search and interactive diagrams. Screenshots and reports stay outside the generated site.
