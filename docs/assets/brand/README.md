# Focal logo

Revised 2026-09-09 with the built-in image generation tool. Following Ada's correction, the
optical body is an oblique triangular prism with a flat base and a visible side face.
Three incoming rays join into one continuous outgoing ray. The broad planes and open cuts
retain the monochrome family style of vorpal and slates.

- `focal-prism-transparent.png` is the original generated artwork with its alpha channel.
- `focal-prism-light.svg` and `focal-prism-dark.svg` embed that PNG unchanged. Their filters
  use the sibling brands' theme colors (`#1f2328` and `#f0f6fc`). The viewBox frames the
  visible bounds with a small margin. These are presentation wrappers around raster
  artwork, not traced vectors.
- The README renders the mark at 276 × 90 pixels. Its width follows the wider prism and ray
  composition while retaining the family's 90-pixel display height.
- `focal-prism-preview.png` shows both theme SVGs enlarged and at README size. It was rendered
  with the installed `rsvg-convert` on 2026-09-09 and visually checked. ImageMagick inspection
  confirmed an RGBA source with transparent and opaque pixels.

The root README uses relative paths and a `<picture>` element to select the matching theme.
Clicking the logo opens the preview. No external image host or font is required.

## Previous concepts

The hexagonal relay (`9f690b0`) was rejected for resemblance to familiar AI branding.
The tuning fork (`689842a`) was rejected as an unsuitable, insufficiently futuristic metaphor.
The first optical mark (`38d990a`) looked like a diamond and ended in a dot; Ada requested a
prism with an outgoing ray. Two edit attempts for this revision produced flattened RGB
checkerboards and were discarded. The final generation supplies actual alpha transparency.
Previous installed artwork remains in git history.

## Final generation prompt

```text
Generate a finished logo as a PNG with a genuinely transparent alpha background.

The logo depicts an oblique TRIANGULAR GLASS PRISM converting three incoming light bands into one continuous outgoing light band. Pure solid black shapes, clean transparent gaps, precise antialiased edges.

The prism is an extruded optical wedge seen from three-quarter perspective. Show a large simple triangular end face on the left and a long quadrilateral side face receding to the right. The prism has a broad flat base and one top ridge. Its silhouette is squat and architectural, with no downward point and no gemstone shape. Only two broad faces, with a strong transparent seam defining the depth.

Three thick parallel black light bands approach from the left. Their paths continue as clean cutouts through the front face, bend together across the side face, and merge into ONE long straight black output band extending well beyond the right side. The output has a flat end. There is NO DOT or other symbol at the exit. The three input bands and one output band should be equally bold and clearly visible.

Design this as an elegant futuristic emblem for Focal, in the same family as a monochrome blade or sharply cut stacked slates: a memorable physical silhouette with engineered negative space. Sparse, balanced, strong at 90 pixels high. Wide overall proportions; center the mark on the canvas with generous clear margins.

Background: actual alpha transparency outside every black shape and within every cutout. Flat pure black artwork. No backdrop of any kind, no texture, no lighting, no shadow, no gray shading, no rainbow, no spectrum, no glow, no words, no labels, no arrows, no node, no circle, no star, no jewel facets, no diamond, no additional decorative objects. One logo only.
```
