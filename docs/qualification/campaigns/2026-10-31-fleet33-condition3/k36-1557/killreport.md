## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 2.2 s, started again at 37.6 s; serving 38.6 to 41.6 s (12 of 12), 0.9 to 4.0 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 9e8fd82e | focal1* focal5 focal6* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 232 | 38.3 (0.7–38.9) | 78 / 40.4 |
| 1 | ae74b9af | focal4 focal12 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 228 | 0.8 (1.3–2.1) | 78 / 40.4 |
| 2 | fbbe7d4c | focal7* focal20 focal30 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 270 | 2.5 (0.7–3.2) | 78 / 40.4 |
| 3 | 33280fcb | focal10 focal15 focal23* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 247 | 0.8 (1.2–2.1) | 93 / 55.4 |
| 4 | 66678736 | focal13 focal17* focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 244 | 40.2 (0.7–40.9) | 78 / 40.4 |
| 5 | 8f230c72 | focal16* focal32* focal33 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 222 | 37.5 (0.7–38.1) | 77 / 39.4 |
| 6 | 3ca86583 | focal2 focal18 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 251 | 0.9 (1.1–2.1) | 76 / 38.4 |
| 7 | 2648071c | focal9* focal11 focal22 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 231 | 0.6 (1.5–2.1) | 90 / 52.4 |
| 8 | 661ae10e | focal3 focal25 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 241 | 0.6 (1.5–2.1) | 79 / 41.4 |
| 9 | a8fa548f | focal8* focal21 focal28* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 275 | 41.4 (0.7–42.1) | 76 / 38.4 |
| 10 | 2ce51493 | focal14 focal24 focal31* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 230 | 2.0 (0.7–2.7) | 78 / 40.4 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 100.000%
writes (measured window): committed 291,518, refused 0, unknown 0; committed p50/p99/p99.9/max 100.6/4959.4/37202.9/43230.5 ms; schedule delay p99 33.8 ms; wall 136.9 s
refusals (reasons): []

writes not committed, by start (5 s windows from the kill): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
