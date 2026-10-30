## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 12.3 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | c53691d4 | focal1* focal15 focal20 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 158 | 10.0 (0.1–10.1) | 77 / 64.7 |
| 1 | 3fbf34ec | focal4 focal24 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 166 | 0.5 (0.6–1.1) | 83 / 70.7 |
| 2 | aba5cf4e | focal3 focal5 focal7* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 116 | 10.0 (0.1–10.1) | 82 / 69.7 |
| 3 | d2049c19 | focal11 focal10 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 142 | 0.5 (0.2–0.7) | 43 / 30.7 |
| 4 | 8f099489 | focal9* focal13 focal23* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 124 | 13.6 (0.1–13.7) | 90 / 77.7 |
| 5 | e8daa64a | focal8* focal16* focal21 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 129 | 12.4 (0.1–12.5) | 85 / 72.7 |
| 6 | b946b193 | focal12 focal14 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 153 | 0.5 (0.8–1.4) | 77 / 64.7 |
| 7 | 8bdc8ac3 | focal6* focal17* focal22 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 140 | 13.1 (0.1–13.2) | 77 / 64.7 |
| 8 | c6408e56 | focal2 focal18 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 154 | 0.5 (0.9–1.4) | 54 / 41.7 |
| 9 | 6e33f7f0 | focal27* focal28* focal29 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 141 | 13.4 (0.1–13.5) | 86 / 73.7 |
| 10 | 245ae4ae | focal30 focal32* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 154 | 12.5 (0.1–12.6) | 92 / 79.7 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 100.000%
writes (measured window): committed 291,593, refused 0, unknown 0; committed p50/p99/p99.9/max 37.5/897.8/11081.3/20043.1 ms; schedule delay p99 4.6 ms; wall 135.9 s
refusals (reasons): []

writes not committed, by start (5 s windows from the isolation): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
