#!/usr/bin/env python3
"""Compose Xenon's brand exports from the canonical symbol and bundled Space Grotesk font.

Requires rsvg-convert 2.62.3 and ImageMagick 7.1.2-31 for raster exports.
Requires fonttools 4.61.1; the pinned font is bundled in assets/fonts/.
"""

from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET
from functools import lru_cache
from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.transformPen import TransformPen
from fontTools.pens.teePen import TeePen

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


@lru_cache(maxsize=None)
def font(weight):
    return instantiateVariableFont(TTFont(ROOT / "assets/fonts/SpaceGrotesk.ttf"),
                                   {"wght": weight}, inplace=True)


def outline(text, size, weight=400, tracking=0):
    face = font(weight)
    glyphs = face.getGlyphSet()
    cmap = face.getBestCmap()
    scale = size / face["head"].unitsPerEm
    paths = SVGPathPen(glyphs)
    bounds = BoundsPen(glyphs)
    x = 0
    for char in text:
        glyph = glyphs[cmap[ord(char)]]
        glyph.draw(TransformPen(TeePen([paths, bounds]), (scale, 0, 0, -scale, x, 0)))
        x += glyph.width * scale + tracking
    return paths.getCommands(), bounds.bounds, x - tracking


def label(text, x, y, size, color, weight=400, tracking=0, end=False):
    path, _, width = outline(text, size, weight, tracking)
    if end:
        x -= width
    return f'<path transform="translate({x} {y})" fill="{color}" d="{path}"/>'


def main():
    path, (left, top, right, bottom), _ = outline("xenon", 1000, 600, -20)
    svg("wordmark.svg", right - left, bottom - top, "xenon",
        f'<path transform="translate({-left} {-top})" fill="#000000" d="{path}"/>')
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
            + label("Temporal persistence.", 88, 300, 43, ink, 600, -1)
            + label("Built on object storage.", 88, 355, 43, muted, 400, -1)
            + f'<path d="M88 432H1512" stroke="{line}"/>'
            + label("Go nodes. SlateDB. S3.", 88, 481, 20, muted)
            + label("IN DEVELOPMENT", 1512, 481, 17, muted, 500, 1.6, end=True)
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
    print("Brand exports built from mark.svg and bundled Space Grotesk")


if __name__ == "__main__":
    main()
