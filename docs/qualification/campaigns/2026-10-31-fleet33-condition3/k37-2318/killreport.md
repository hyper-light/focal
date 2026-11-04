## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.2 s, started again at 34.2 s; serving 35.0 to 36.5 s (12 of 12), 0.8 to 2.2 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 88679e1e | focal1* focal6* focal17* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 132 | 37.4 (0.2–37.6) | 90 / 55.8 |
| 1 | 00d0006c | focal4 focal29 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 133 | 0.5 (9.6–10.1) | 93 / 58.8 |
| 2 | c01b5f51 | focal7* focal26 focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 132 | 35.1 (0.2–35.3) | 94 / 59.8 |
| 3 | 80336751 | focal10 focal11 focal24 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 127 | 0.5 (12.8–13.3) | 94 / 59.8 |
| 4 | 9efb9c3a | focal9* focal13 focal23* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 135 | 35.8 (0.2–36.0) | 94 / 59.8 |
| 5 | 2685294d | focal16* focal30 focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 94 | 35.1 (0.2–35.3) | 94 / 59.8 |
| 6 | 3fe5a421 | focal2 focal3 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 132 | 0.5 (6.9–7.5) | 94 / 59.8 |
| 7 | 94228123 | focal20 focal21 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 89 | 0.5 (0.3–0.8) | 94 / 59.8 |
| 8 | 4d2acf70 | focal8* focal15 focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 132 | 0.5 (0.5–1.0) | 93 / 58.8 |
| 9 | 48b11fa5 | focal5 focal12 focal28* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 122 | 2.5 (0.2–2.7) | 93 / 58.8 |
| 10 | baa4b44e | focal14 focal18 focal31* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 103 | 2.5 (0.2–2.7) | 94 / 59.8 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 100.000%
writes (measured window): committed 291,303, refused 0, unknown 0; committed p50/p99/p99.9/max 39.1/14960.6/36300.7/41788.7 ms; schedule delay p99 12.3 ms; wall 136.6 s
refusals (reasons): []

writes not committed, by start (5 s windows from the kill): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
