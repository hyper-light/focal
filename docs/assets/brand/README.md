# Focal logo

Revised 2026-09-09 with the built-in image generation tool. A futuristic tuning fork
represents independent agents finding agreement. The index cuts in its stem suggest the
ordered record of their exchanges. Focal's [peer contract](../../archictecutre/16-peer-validation-contract.md)
keeps execution with participants and records their requirements, evidence, and evaluations.
The instrument is a visual metaphor for that coordination.

Its diagonal silhouette and precise cutouts belong to the same family as vorpal's blade and
slates' floating planes. The initial hexagonal relay mark (commit `9f690b0`) was rejected on
2026-09-09 for its resemblance to familiar AI branding and weak connection to Focal. That
artwork remains in git history; the README now uses the tuning fork.

- `focal-fork-transparent.png` is the original generated artwork, including its alpha channel.
- `focal-fork-light.svg` and `focal-fork-dark.svg` embed that PNG unchanged. Their filters use
  the sibling brands' theme colors (`#1f2328` and `#f0f6fc`). Their common square viewBox
  centers the visible bounds with a small margin. These are presentation wrappers around
  raster artwork, not traced vectors.
- `focal-fork-preview.png` shows both SVGs at 256 pixels and the README's 90-pixel size.
  It was rendered with the installed `rsvg-convert` on 2026-09-09 and visually checked.

The root README uses relative paths and a `<picture>` element to select the matching theme.
Clicking its logo opens the preview. No external image host or font is required.

## Generation prompt

```text
Use case: logo-brand.
Design one striking, original futuristic tuning-fork emblem for Focal, an inter-agent claims ledger: independent agents communicate, provide evidence, and verify results while their exchanges are recorded.
Creative concept: a beautifully machined coordination instrument, a tuning fork from a restrained science-fiction world. Two long independent tines rise from one short, solid stem. They have faceted, subtly unequal chisel-cut tips and a generous narrow slot between them. A tiny square negative-space cut at the base of the slot suggests a recorded unit of evidence. Three broad, perfectly aligned shallow index cuts along one side of the stem suggest the ordered ledger. A bold and memorable silhouette: long elegant prongs, crisp internal channel, tapered solid handle with an angled end. The tool leans diagonally from lower left to upper right, giving it the poise and sharp graphic presence of an elegant blade. It must remain immediately recognizable as a forked calibration instrument, not a letter or a circuit.
Art direction: monumental simplicity with exquisitely judged proportions and a little asymmetry. Futuristic precision expressed only through geometry, not through surface detail. This is a sibling of vorpal's monochrome blade mark and slates' sharply cut floating planes. Strong solid masses, one beautiful negative-space slot, confident cut edges. The ornament is functional-looking indexing, not decorative filigree.
Pure flat black silhouette on genuinely transparent background. Internal cuts also transparent. Crisp antialiased edges. One single isolated object centered on a square canvas, generous breathing room but fills most of its height.
No text, wordmark, lettering, curved scrolls, plants, branches, leaves, ribbons, circuitry, gradients, shading, glow, texture, shadows, outline-only drawing, or white-filled cutouts. No ring, hexagonal badge, rotational symmetry, interwoven knot, camera iris, recycling arrows, chat bubbles, network nodes, or abstract corporate emblem. No floating jewel or star. No weapon blade or spear tips. Keep broad cuts legible at 90 pixels high.
```
