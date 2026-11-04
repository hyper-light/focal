## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 30.4 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 3115eef4 | focal1* focal2 focal3 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 163 | 1.5 (0.1–1.6) | 94 / 63.6 |
| 1 | d6335958 | focal4 focal15 focal14 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 172 | 0.5 (0.7–1.2) | 93 / 62.6 |
| 2 | 25571b31 | focal7* focal30 focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 222 | 32.4 (0.1–32.6) | 80 / 49.6 |
| 3 | 506f6289 | focal5 focal10 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 172 | 0.5 (0.9–1.4) | 93 / 62.6 |
| 4 | c90d843b | focal11 focal13 focal21 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 191 | 0.5 (0.8–1.3) | 86 / 55.6 |
| 5 | f4759399 | focal16* focal18 focal20 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 171 | 1.5 (0.1–1.6) | 92 / 61.6 |
| 6 | 9f931dbb | focal9* focal19 focal26 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 189 | 0.5 (0.4–0.9) | 88 / 57.6 |
| 7 | 61d05c55 | focal6* focal8* focal22 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 158 | 30.7 (0.1–30.8) | 93 / 62.6 |
| 8 | 206e5ad9 | focal25 focal29 focal27* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 160 | 0.5 (0.2–0.8) | 94 / 63.6 |
| 9 | 78370291 | focal12 focal23* focal28* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 156 | 31.0 (0.1–31.1) | 91 / 60.6 |
| 10 | d31a4a5c | focal17* focal24 focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 170 | 30.9 (0.1–31.0) | 94 / 63.6 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 100.000%
writes (measured window): committed 291,374, refused 0, unknown 0; committed p50/p99/p99.9/max 54.0/1238.5/28493.1/33861.0 ms; schedule delay p99 6.7 ms; wall 136.7 s
refusals (reasons): []

writes not committed, by start (5 s windows from the isolation): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
