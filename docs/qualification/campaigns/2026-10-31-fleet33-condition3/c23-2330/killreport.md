## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 12.4 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | c734f89a | focal1* focal2 focal6* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 112 | 12.6 (0.1–12.8) | 41 / 28.6 |
| 1 | 4210b178 | focal4 focal9* focal14 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 138 | 0.5 (0.6–1.2) | 67 / 54.6 |
| 2 | 774a4927 | focal7* focal17* focal21 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 130 | 13.8 (0.1–13.9) | 69 / 56.6 |
| 3 | b12e6656 | focal10 focal18 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 133 | 0.5 (0.8–1.3) | 70 / 57.6 |
| 4 | 118f512c | focal5 focal13 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 126 | 0.5 (0.3–0.9) | 74 / 61.6 |
| 5 | d2008b4a | focal11 focal16* focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 165 | 13.8 (0.1–13.8) | 18 / 5.6 |
| 6 | 8c585a46 | focal19 focal24 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 131 | 0.5 (0.3–0.8) | 74 / 61.6 |
| 7 | 462a63c2 | focal15 focal20 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 126 | 0.5 (0.5–1.0) | 92 / 79.6 |
| 8 | 36ea0fea | focal3 focal8* focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 137 | 0.5 (0.5–1.0) | 88 / 75.6 |
| 9 | 09ffdb42 | focal28* focal33 focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 132 | 13.0 (0.1–13.1) | 88 / 75.6 |
| 10 | 4746690a | focal12 focal23* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 117 | 12.4 (0.1–12.5) | 80 / 67.6 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 100.000%
writes (measured window): committed 291,284, refused 0, unknown 0; committed p50/p99/p99.9/max 31.1/216.4/11034.0/15205.6 ms; schedule delay p99 2.0 ms; wall 135.7 s
refusals (reasons): []

writes not committed, by start (5 s windows from the isolation): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
