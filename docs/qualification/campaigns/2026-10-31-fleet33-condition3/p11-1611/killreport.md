## focal-220-128-work-agents500-nodes33-retry-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 30.3 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 3e0fe57c | focal1* focal2 focal3 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 19 | 163 | 1.7 (0.1–1.8) | 91 / 60.7 |
| 1 | cd222731 | focal4 focal21 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 125 | 0.5 (90.6–91.0) | 93 / 62.7 |
| 2 | 7f62bc0e | focal7* focal17* focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 244 | 123 | 32.2 (0.1–32.3) | 93 / 62.7 |
| 3 | 7f52157b | focal6* focal10 focal14 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 145 | 0.5 (1.0–1.5) | 93 / 62.7 |
| 4 | 50b74a67 | focal5 focal13 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 127 | 0.6 (0.9–1.4) | 93 / 62.7 |
| 5 | b3600017 | focal15 focal16* focal20 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 84 | 138 | 1.5 (0.1–1.6) | 94 / 63.7 |
| 6 | b2be758f | focal18 focal19 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 151 | 0.5 (0.3–0.8) | 93 / 62.7 |
| 7 | 641601a7 | focal8* focal12 focal22 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 10 | 112 | 0.6 (1.0–1.6) | 93 / 62.7 |
| 8 | 6b9d075a | focal9* focal11 focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 118 | 0.5 (0.3–0.8) | 94 / 63.7 |
| 9 | 0a8e080d | focal28* focal30 focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 47 | 85 | 30.5 (0.1–30.6) | 94 / 63.7 |
| 10 | d042db1f | focal23* focal24 focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 45 | 94 | 31.0 (0.1–31.1) | 94 / 63.7 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 449; judged/started 100.000%
writes (measured window): committed 291,463, refused 0, unknown 0; committed p50/p99/p99.9/max 37.1/9443.1/29677.3/37180.4 ms; schedule delay p99 9.6 ms; wall 136.5 s
refusals (reasons): []

writes not committed, by start (5 s windows from the isolation): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
