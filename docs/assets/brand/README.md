# Focal logo

Revised 2026-09-09 with the built-in image generation tool, following Ada's direction:
a prism-like artifact bending light toward a focus. Three separate beams enter a faceted lens
and converge on one point. The crystalline form and open cuts express focus through the same
monochrome silhouette language as vorpal's blade and slates' floating planes.

- `focal-focus-transparent.png` is the original generated artwork, including its alpha channel.
- `focal-focus-light.svg` and `focal-focus-dark.svg` embed that PNG unchanged. Their filters
  use the sibling brands' theme colors (`#1f2328` and `#f0f6fc`); their common square viewBox
  frames the visible silhouette. These are presentation wrappers around raster artwork,
  not traced vectors.
- `focal-focus-preview.png` shows both SVGs at 256 pixels and the README's 90-pixel size.
  It was rendered with the installed `rsvg-convert` on 2026-09-09 and visually checked.

The root README uses relative paths and a `<picture>` element to select the matching theme.
Clicking its logo opens the preview. No external image host or font is required.

## Previous concepts

The hexagonal relay (commit `9f690b0`) was rejected for resemblance to familiar AI branding
and a weak connection to Focal. The tuning fork (`689842a`) was rejected as an unsuitable,
insufficiently futuristic metaphor. Their artwork remains in git history; the current README
uses only the optical mark.

## Generation prompt

```text
Use case: logo-brand.
Create one finished monochrome futuristic logo for Focal. The user's chosen concept is a sharp prism-like optical artifact bending separate light beams into a single focal point. Focal is an inter-agent communication protocol and ledger; the visual identity should express FOCUS, clarity, and precise convergence.
Main subject: a compact, beautifully faceted crystalline focusing element viewed obliquely from the side. It has the bold silhouette and hard-cut faces of an optical prism, but functions visually as a faceted lens. Several separated incoming rays pass into its left face; their paths change direction through the facets and converge to ONE small precise focal point beyond its right face. Make convergence unmistakable: the outgoing rays draw closer together and meet, never fan outward.
Design this as a sophisticated logo, not a science diagram. The optical body is the dominant solid mass. Three broad, clean ray strokes and one focal point are enough. Bold angular facets are separated by generous transparent channels. Integrate the beam paths with the body's negative-space cuts to create one coherent, memorable silhouette. Slight upward diagonal motion, controlled asymmetry, engineered proportions, sharp beveled edges. A tangible futuristic optical instrument, not a triangle badge.
Pure flat black silhouette on genuinely transparent background; all internal channels transparent. Crisp antialiased edges, no gray shading. The form must remain clear at 90 pixels tall.
This belongs beside vorpal's elegant monochrome blade and slates' sharp floating planes: restrained black silhouettes and beautiful negative space, each depicting its own object.
Avoid the familiar front-facing triangle with a beam and rainbow: no rainbow, spectrum, color, Pink Floyd composition, equilateral triangle outline, hexagonal seal, knot, rotational emblem, tuning fork, chat bubbles, arrows, labels, lettering, wordmark, decorative stars, circuitry, gradients, glow, shadows, texture, wireframe, fine hairlines, or background scene.
One single centered logo on square canvas, occupying most of the canvas with modest transparent margins. Do not include alternative designs or mockups.
```
