## focal-220-128-work-agents500-nodes33-retry-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 12.3 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | ce2ed7df | focal1* focal14 focal24 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 156 | 1.5 (0.1–1.6) | 68 / 55.7 |
| 1 | 9425d8be | focal2 focal4 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 117 | 0.6 (1.1–1.7) | 80 / 67.7 |
| 2 | dfc420f6 | focal8* focal7* focal33 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 86 | 109 | 13.4 (0.1–13.6) | 69 / 56.7 |
| 3 | a001528f | focal9* focal10 focal17* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 170 | 12.9 (0.1–13.0) | 68 / 55.7 |
| 4 | 17880f07 | focal11 focal13 focal15 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 152 | 0.5 (0.3–0.9) | 73 / 60.7 |
| 5 | 53d424d3 | focal16* focal26 focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 150 | 12.7 (0.1–12.8) | 71 / 58.7 |
| 6 | 971cbcac | focal5 focal19 focal18 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 120 | 0.5 (0.9–1.4) | 68 / 55.7 |
| 7 | 25affe7b | focal3 focal22 focal23* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 127 | 0.6 (67.7–68.2) | 69 / 56.7 |
| 8 | 4bfc6661 | focal6* focal25 focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 103 | 14.4 (0.1–14.5) | 69 / 56.7 |
| 9 | 2cec19f5 | focal12 focal28* focal29 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 141 | 1.5 (0.1–1.5) | 92 / 79.7 |
| 10 | 1a05004e | focal20 focal21 focal31* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 164 | 2.5 (0.1–2.6) | 69 / 56.7 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 86; judged/started 100.000%
writes (measured window): committed 291,584, refused 0, unknown 0; committed p50/p99/p99.9/max 40.3/1661.5/10984.4/15726.5 ms; schedule delay p99 5.7 ms; wall 135.9 s
refusals (reasons): []

writes not committed, by start (5 s windows from the isolation): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
