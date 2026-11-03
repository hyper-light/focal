## focal-220-128-work-agents500-nodes33-retry-faults.json

killed (12): focal2 focal8 focal9 focal10 focal12 focal19 focal20 focal23 focal27 focal30 focal31 focal33; all killed by 1.6 s, started again at 66.4 s; serving 66.9 to 69.3 s (12 of 12), 0.5 to 2.9 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | ae78e1ed | focal1 focal6 focal11 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 614 | 0.4 (99.6–100.0) | 97 / 30.6 |
| 1 | 46fe3614 | focal4 focal32 focal33* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 445 | 9.6 (83.5–93.1) | 100 / 33.6 |
| 2 | 8ebd7068 | focal7 focal9* focal14 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 395 | 543 | 20.5 (49.2–69.7) | 97 / 30.6 |
| 3 | b2e084dd | focal3 focal5 focal10* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 295 | 460 | 17.7 (49.1–66.8) | 97 / 30.6 |
| 4 | ab05c879 | focal8* focal13 focal18 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 457 | 0.3 (89.7–90.1) | 97 / 30.6 |
| 5 | f3c00717 | focal16 focal20* focal24 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 373 | 581 | 18.2 (49.3–67.5) | 95 / 28.6 |
| 6 | 23d84827 | focal19* focal26 focal30* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 453 | 583 | 66.2 (0.2–66.4) | 97 / 30.6 |
| 7 | 5cd07f71 | focal15 focal22 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 479 | 0.6 (89.7–90.3) | 97 / 30.6 |
| 8 | 69ef311e | focal12* focal17 focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 491 | 5.4 (51.7–57.0) | 97 / 30.6 |
| 9 | d23f30d6 | focal2* focal21 focal28 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 523 | 538 | 16.9 (52.4–69.3) | 107 / 40.6 |
| 10 | 08d940bd | focal23* focal27* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 924 | 491 | 68.2 (0.2–68.4) | 97 / 30.6 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 2963; judged/started 100.000%
writes (measured window): committed 295,330, refused 0, unknown 0; committed p50/p99/p99.9/max 332.2/47605.1/67132.2/70875.3 ms; schedule delay p99 12407.9 ms; wall 147.1 s
refusals (reasons): []

writes not committed, by start (5 s windows from the kill): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
