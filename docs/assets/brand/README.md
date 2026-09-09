# Focal logo

Redrawn directly as SVG on 2026-09-09 after Ada supplied prism references. The logo uses the
simple end-on triangular profile shown in the
[Newton artwork](https://deltavcreations.com/portfolio/prism-light-spectrum-artwork-newton/).
Three incoming rays meet at the right face and continue as one outgoing ray, following Ada's
requested focus direction. The front interior is transparent and every segment is straight.
A shallow side face adds depth, with a faint fill and lighter rear edges.

## Files and construction

- `focal-prism.svg` is the editable monochrome vector master.
- `focal-prism-light.svg` and `focal-prism-dark.svg` contain the same geometry in the sibling
  brands' theme colors, `#1f2328` and `#f0f6fc`. They contain vector paths directly.
- `focal-prism-transparent.png` is a 1164 × 540 transparent export from the vector master,
  rendered with the installed `rsvg-convert`.
- `focal-prism-preview.png` shows both theme SVGs enlarged and at their 194 × 90 README size.
  Both views were visually checked on 2026-09-09.

The triangle vertices are (300, 36), (168, 264), and (432, 264). Each ray enters at its
intersection with the left side. The shared exit is the intersection of the right side
with y = 158. The SVG contains straight line segments only. Its viewBox preserves the
drawing's proportions as it scales.

The rear triangle is translated by (28, -10). Only the right side face is visible: its
joining edges are parallel, and its rear edge is parallel to the front edge. The side
fill uses 8% ink opacity and its far edges use 55%. The accepted front profile and all
ray paths are unchanged.

The root README selects a theme with a `<picture>` element and links to the preview.
The artwork has no external images, fonts, or scripts.

## Supplied references

- [Delta V Créations — Newton](https://deltavcreations.com/portfolio/prism-light-spectrum-artwork-newton/):
  the full artwork and detail image were visually inspected; its open triangular profile
  informed this redraw.
- [StockCake — Colorful Prism Art](https://stockcake.com/i/colorful-prism-art_532801_410392):
  page text was accessible; the image fetch was blocked.
- [Shutterstock prism icon](https://www.shutterstock.com/image-vector/prism-icon-art-illustrations-premium-line-2700688405):
  the image page could not be retrieved.
- [Etsy prism art prints](https://www.etsy.com/market/prism_art_print):
  the supplied collection could not be retrieved directly.

No reference artwork is embedded in these assets.

## Previous concepts

Earlier generated concepts remain in git history: the relay (`9f690b0`), tuning fork
(`689842a`), diamond with a terminal dot (`38d990a`), and tapered wedge with curved paths
(`53948e6`). This redraw replaces the generated geometry with the simple profile from
Ada's reference.
