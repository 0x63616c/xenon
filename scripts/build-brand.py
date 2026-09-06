#!/usr/bin/env python3
"""Compose Xenon's brand exports from the two canonical SVGs.

Requires rsvg-convert 2.62.3 and ImageMagick 7.1.2-31 for raster exports.
The SVG composition itself uses only Python's standard library.
"""

from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "assets/brand"
NS = "{http://www.w3.org/2000/svg}"
ET.register_namespace("", NS[1:-1])


def fragment(name, color):
    root = ET.parse(OUT / name).getroot()
    for child in list(root):
        if child.tag == NS + "title":
            root.remove(child)
    return "".join(ET.tostring(c, encoding="unicode") for c in root).replace(
        "#000000", color
    )


def svg(name, width, height, title, body):
    text = (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" role="img" aria-labelledby="title">'
        f'<title id="title">{title}</title>{body}</svg>\n'
    )
    ET.fromstring(text)
    (OUT / name).write_text(text)


def place(content, x, y, scale):
    return f'<g transform="translate({x} {y}) scale({scale})">{content}</g>'


def main():
    _, _, word_w, word_h = map(
        float, ET.parse(OUT / "wordmark.svg").getroot().attrib["viewBox"].split()
    )
    for suffix, ink, paper in [("", "#000000", "#ffffff"), ("-white", "#ffffff", "#000000")]:
        mark = fragment("mark.svg", ink)
        word = fragment("wordmark.svg", ink)
        if suffix:
            svg("mark-white.svg", 512, 512, "Xenon orbital X", mark)
            svg("wordmark-white.svg", word_w, word_h, "xenon", word)
        word_scale = 100 / word_h
        svg(
            f"lockup{suffix}.svg", 230 + word_w * word_scale, 192, "Xenon",
            place(mark, 0, 0, 192 / 512) + place(word, 222, 46, word_scale),
        )
        # Text is meaningful at README width; the mark stays away from the copy.
        muted = "#606060" if not suffix else "#a3a3a3"
        line = "#e6e6e6" if not suffix else "#333333"
        banner = (
            f'<rect width="1600" height="540" fill="{paper}"/>'
            + place(word, 88, 86, 470 / word_w)
            + place(mark, 1110, 40, 0.72)
            + f'<g font-family="Arial,Helvetica,sans-serif">'
            f'<text x="88" y="300" font-size="43" font-weight="600" '
            f'letter-spacing="-1.4" fill="{ink}">Temporal persistence.</text>'
            f'<text x="88" y="355" font-size="43" letter-spacing="-1.4" '
            f'fill="{muted}">Built on object storage.</text>'
            f'<path d="M88 432H1512" stroke="{line}"/>'
            f'<text x="88" y="481" font-size="20" fill="{muted}">Go nodes. SlateDB. S3.</text>'
            f'<text x="1512" y="481" text-anchor="end" font-size="17" '
            f'letter-spacing="1.6" fill="{muted}">IN DEVELOPMENT</text></g>'
        )
        svg("banner-dark.svg" if suffix else "banner.svg", 1600, 540,
            "Xenon — Temporal persistence. Built on object storage. In development.", banner)
        subprocess.run(["rsvg-convert", "-w", "1024", "-h", "1024", "-o",
                        str(OUT / f"mark{suffix}.png"), str(OUT / f"mark{suffix}.svg")], check=True)
    for name in ["banner", "banner-dark"]:
        subprocess.run(["rsvg-convert", "-o", str(OUT / f"{name}.png"),
                        str(OUT / f"{name}.svg")], check=True)
    mark = fragment("mark.svg", "#000000")
    svg("favicon.svg", 512, 512, "Xenon", '<rect width="512" height="512" rx="96" fill="#ffffff"/>' + mark)
    subprocess.run(["rsvg-convert", "-w", "180", "-h", "180", "-o",
                    str(OUT / "apple-touch-icon.png"), str(OUT / "favicon.svg")], check=True)
    subprocess.run(["magick", str(OUT / "apple-touch-icon.png"), "-define",
                    "icon:auto-resize=48,32,16", str(OUT / "favicon.ico")], check=True)
    print("Brand exports built from assets/brand/mark.svg and wordmark.svg")


if __name__ == "__main__":
    main()
