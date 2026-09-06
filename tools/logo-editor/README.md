# Xenon logo editor

Run `python3 -m http.server 4193 --bind 127.0.0.1` from the repository root,
then open http://127.0.0.1:4193/tools/logo-editor/.

`editor.fragment.html` is the editable source. `index.html` is the generated
standalone editor, without a Codex runtime dependency. Rebuild it and the SVG
logo/spinners with `node scripts/build-logo.mjs` from the repository root.

The editor imports pinned Three.js 0.180.0 from jsDelivr, its GLB exporter from
esm.sh, and Space Grotesk from Google Fonts. It needs network access for those
imports. The generated logo and SVG loading spinners need no JavaScript or CDN.

Controls include spoke width, tip and inner-corner roundness, layer count,
sidewall and outline thickness, spacing, camera, individual-disc rotation,
and editable cubic Bezier timing curves. Reduced-motion preferences pause
playback initially. Play/Pause is at the top.

The approved defaults are recorded in `assets/brand/shape.json`.
Use **Save motion settings** to export a tuning preset. **Export current pose**
exports a static GLB, including the side outlines for the current camera;
it does not bake animation or camera-dependent outlines into a universal model.

The editor's live controls do not rewrite repository files. To adopt new
branding defaults, update the source defaults and `scripts/build-logo.mjs`,
then regenerate. See `docs/brand.md` for all brand exports.
