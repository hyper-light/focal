## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal4 focal9 focal10 focal11 focal12 focal19 focal20 focal23 focal27 focal30 focal31 focal33; all killed by 1.3 s, started again at 64.3 s; serving 64.8 to 67.1 s (12 of 12), 0.5 to 2.8 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1fdf4178 | focal1 focal2 focal6 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 173 | 0.2 (33.8–33.9) | 80 / 15.7 |
| 1 | ddcccc26 | focal4* focal26 focal30* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 262 | 65.6 (0.1–65.7) | 78 / 13.7 |
| 2 | 008e878e | focal7 focal33* focal32 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 265 | 4.6 (54.5–59.1) | 77 / 12.7 |
| 3 | ceccf783 | focal3 focal10* focal29 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 174 | 1.5 (0.1–1.6) | 80 / 15.7 |
| 4 | 9c4953f2 | focal13 focal15 focal20* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 154 | 0.2 (33.8–33.9) | 80 / 15.7 |
| 5 | 5ca159e4 | focal11* focal16 focal18 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 160 | 5.4 (39.0–44.4) | 79 / 14.7 |
| 6 | a0105152 | focal14 focal19* focal24 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 153 | 1.3 (0.1–1.4) | 88 / 23.7 |
| 7 | 11f6b836 | focal5 focal9* focal22 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 177 | 7.5 (55.0–62.5) | 90 / 25.7 |
| 8 | 90e994aa | focal12* focal17 focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 160 | 0.2 (33.8–33.9) | 80 / 15.7 |
| 9 | fc54bffc | focal23* focal27* focal28 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 168 | 66.3 (0.1–66.5) | 84 / 19.7 |
| 10 | 3b7041d7 | focal8 focal21 focal31* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 166 | 103.0 (92.6–195.6) | 89 / 24.7 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 100.000%
writes (measured window): committed 291,552, refused 0, unknown 0; committed p50/p99/p99.9/max 52.3/7216.5/60589.5/72206.0 ms; schedule delay p99 15.8 ms; wall 243.8 s
refusals (reasons): []

writes not committed, by start (5 s windows from the kill): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
