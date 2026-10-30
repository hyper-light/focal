## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 12.3 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | b5c57337 | focal1* focal3 focal29 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 129 | 10.0 (0.1–10.1) | 83 / 70.7 |
| 1 | 11c1dc5c | focal4 focal23* focal33 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 130 | 0.6 (41.8–42.4) | 77 / 64.7 |
| 2 | 9a7ed01e | focal7* focal11 focal30 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 121 | 10.0 (0.1–10.1) | 92 / 79.7 |
| 3 | e624ff34 | focal10 focal12 focal17* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 128 | 0.6 (41.8–42.4) | 87 / 74.7 |
| 4 | 528c2bfe | focal6* focal13 focal26 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 116 | 0.6 (41.8–42.4) | 77 / 64.7 |
| 5 | 04d93deb | focal8* focal16* focal21 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 153 | 12.7 (0.1–12.8) | 71 / 58.7 |
| 6 | 87436ec5 | focal14 focal15 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 146 | 0.9 (41.8–42.7) | 83 / 70.7 |
| 7 | 86c68d13 | focal5 focal18 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 106 | 0.6 (41.8–42.4) | 92 / 79.7 |
| 8 | 6cbcd25f | focal2 focal9* focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 116 | 0.8 (41.8–42.6) | 92 / 79.7 |
| 9 | 2788b3d1 | focal24 focal28* focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 123 | 13.6 (0.1–13.7) | 89 / 76.7 |
| 10 | 0a08a4b3 | focal20 focal27* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 175 | 13.2 (0.1–13.3) | 43 / 30.7 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 100.000%
writes (measured window): committed 291,617, refused 0, unknown 0; committed p50/p99/p99.9/max 40.4/716.1/10059.8/17113.8 ms; schedule delay p99 4.8 ms; wall 135.9 s
refusals (reasons): []

writes not committed, by start (5 s windows from the isolation): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
