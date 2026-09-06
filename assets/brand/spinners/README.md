# Xenon loading spinners

- `stagger.svg`: three anticlockwise quarter-turns, offset by 0.25 seconds.
- `counter.svg`: alternating directions with staggered easing.
- `orbit.svg`: a continuous anticlockwise turn.
- `*-white.svg`: white outlines and black faces for black surfaces.

Open `index.html` for a comparison at 120, 48, and 24 pixels. SVGs work in an
ordinary `<img>` without Three.js. Each respects `prefers-reduced-motion`.

```html
<span role="status">
  <img src="stagger.svg" width="32" height="32" alt="Loading">
</span>
```

Use the white variant on black; the moving discs have opaque faces so lower
discs are properly hidden. These assets are intended for white/black surfaces,
not arbitrary colored backgrounds. Do not use an animation as the only loading
status indicator. Omit status roles and use empty alt text for decoration.

Regenerate with `node scripts/build-logo.mjs`. Static brand derivatives are
regenerated separately with `python3 scripts/build-brand.py`.

Primary website loader: `../loader.svg` (original Counter, white on black). The website also registers `<XenonLoader />` for loading states. `counter-v2.svg` and `counter-v2-white.svg` add a pulse and remain optional.
