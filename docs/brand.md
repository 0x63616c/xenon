# Xenon brand

![Xenon](../assets/brand/lockup.svg)

Xenon's orbital X combines heavy diagonal bands, squared curves and a woven
center. Use the same mark across the project. Keep layouts monochrome, with
clear typography, generous whitespace and short, factual product copy.

## Assets

| Use | Light surface | Dark surface |
| --- | --- | --- |
| Symbol | [SVG](../assets/brand/mark.svg) · [PNG](../assets/brand/mark.png) | [SVG](../assets/brand/mark-white.svg) · [PNG](../assets/brand/mark-white.png) |
| Symbol + wordmark | [SVG](../assets/brand/lockup.svg) | [SVG](../assets/brand/lockup-white.svg) |
| Wordmark | [SVG](../assets/brand/wordmark.svg) | [SVG](../assets/brand/wordmark-white.svg) |
| Repository banner | [SVG](../assets/brand/banner.svg) · [PNG](../assets/brand/banner.png) | [SVG](../assets/brand/banner-dark.svg) · [PNG](../assets/brand/banner-dark.png) |

Browser assets: [SVG favicon](../assets/brand/favicon.svg),
[ICO favicon](../assets/brand/favicon.ico), and
[Apple touch icon](../assets/brand/apple-touch-icon.png).

SVGs are scalable path artwork. Symbol PNGs are transparent at 1024 × 1024;
banners are 1600 × 540. Wordmarks are outlined so they do not depend on a font
being installed. Banner supporting copy uses Arial / Helvetica / sans-serif.

## Usage

- Use black on white, or white on black. Neutral grays belong in supporting UI.
- Preserve the aspect ratio and the mark's built-in clear space. Leave at least
  one band-width between the symbol and unrelated content.
- Use the full mark at 32 px or larger in interfaces. The supplied 16 px favicon
  retains the silhouette; its inner detail is less distinct at that size.
- Do not redraw the center, stretch the mark, add outlines, gradients or shadows.
- Use the supplied lockup when the company name accompanies the mark. Interface
  text uses the native system sans-serif, with restrained weights and spacing.
- Keep development status and verification limits visible. Branding is not a
  production-readiness or service-availability claim.

## Source and regeneration

`assets/brand/mark.svg` and `assets/brand/wordmark.svg` are the canonical artwork.
All other files in that directory are derived by:

```sh
python3 scripts/build-brand.py
```

The SVG composer uses Python 3's standard library. Raster exports were validated
with librsvg `rsvg-convert` 2.62.3 and ImageMagick 7.1.2-31. It does not download
fonts or assets. Commit regenerated derivatives with any canonical artwork edit.

The mark was generated from the project's Echo and Interlace concepts in response
to Calum's request to combine options 2 and 3, then converted to paths using
Potrace 1.16. The conversion preserved the chosen geometry. The lowercase wordmark
was outlined from locally installed Arial. Earlier third-party examples were
visual references; no third-party logo file is included in this asset pack.

CopyCat's README and website informed the presentation: a clear identity,
concise introduction, useful navigation and restrained spacing. Xenon's copy and
artwork are specific to this project. The repository and these assets remain
private under the project's current release policy.
