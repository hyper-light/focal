# Focal logo

Created 2026-09-09 with the built-in image generation tool. The relay emblem represents
independent participants exchanging claims, evidence, and validation through a shared history,
as described in the [architecture overview](../../archictecutre/README.md). Distinct angular
paths surround an open focal point. Its monochrome silhouette and geometric cutouts place it
in the same visual family as slates and vorpal.

- `focal-relay-transparent.png` is the original generated artwork, including its alpha channel.
- `focal-relay-light.svg` and `focal-relay-dark.svg` embed that PNG unchanged. Their filters
  use the sibling brands' theme colors (`#1f2328` and `#f0f6fc`); their shared viewBox frames
  the silhouette. These are presentation wrappers around raster artwork, not traced vectors.
- `focal-relay-preview.png` shows both SVGs at 256 pixels and the README's 90-pixel size.
  It was rendered with the installed `rsvg-convert` on 2026-09-09 and visually checked.

The root README uses relative paths and a `<picture>` element to select the matching theme.
Clicking its logo opens the preview. No external image host or font is required.

## Generation prompt

```text
Use case: logo-brand.
Create one finished futuristic monochrome logo for Focal, a Rust inter-agent communication protocol and event-driven ledger. Autonomous agents exchange claims, deliver evidence and validate each other's results; Focal records the shared history without running the agents.
Design a faceted relay emblem: three distinct, equally weighted angular paths interleave around a small, clear hexagonal focal opening. The paths suggest messages passing between independent peers and returning with evidence. Precise geometric cuts separate every path. The outer silhouette forms a compact angular hexagonal seal with a few striking beveled breaks; broad positive masses and generous negative-space channels, sophisticated optical balance. A sense of coordinated convergence without a dominant central controller. The mark should feel like a future-machined insignia, ownable and beautifully proportioned.
This is a sibling of slates (floating sharp-edged slate planes with a stepped cut) and vorpal (an elegant solid blade silhouette with negative-space ornament). Keep their bold flat monochrome restraint, but give Focal its own radial, connected identity, not a stack or blade.
Use pure solid black ink on genuinely transparent background; internal openings and separations also transparent. Crisp antialiased contours. No gradients, shading, texture, shadows, lighting effects, or white-filled holes.
No text, lettermark, wordmark, plants, ribbons, flowing calligraphy, literal camera, shutter blades, crosshairs, arrows, recycling icon, atom, brain, robot, circuitry, or node-and-line network diagram. Avoid a generic blockchain badge or OpenAI knot. Geometry should feel intentional, not complicated.
One single emblem, centered in a square canvas with modest clear margin, occupying most of the image. Readable at 90 pixels tall.
```
