## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.5 s, joined again at 31.0 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 83f55741 | focal1* focal2 focal3 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 101 | 2.4 (0.4–2.9) | 94 / 63.0 |
| 1 | 815baa6e | focal4 focal11 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 122 | 0.5 (1.0–1.6) | 94 / 63.0 |
| 2 | f1d802f2 | focal7* focal32* focal33 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 119 | 31.3 (0.4–31.7) | 94 / 63.0 |
| 3 | 4c6e8741 | focal10 focal21 focal20 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 93 | 0.5 (1.2–1.7) | 94 / 63.0 |
| 4 | 4373dadf | focal5 focal13 focal18 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 123 | 0.5 (1.3–1.8) | 94 / 63.0 |
| 5 | 62677fb5 | focal16* focal26 focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 134 | 31.9 (0.4–32.3) | 94 / 63.0 |
| 6 | 4b9735dc | focal17* focal19 focal24 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 99 | 0.5 (0.6–1.1) | 93 / 62.0 |
| 7 | 65f27184 | focal15 focal22 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 124 | 0.5 (0.4–0.9) | 94 / 63.0 |
| 8 | 06896a27 | focal9* focal14 focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 103 | 0.5 (0.8–1.4) | 93 / 62.0 |
| 9 | 0cb9ca19 | focal8* focal12 focal28* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 128 | 32.5 (0.4–32.9) | 94 / 63.0 |
| 10 | eb6dee16 | focal6* focal23* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 101 | 34.9 (0.4–35.3) | 94 / 63.0 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 100.000%
writes (measured window): committed 291,300, refused 0, unknown 0; committed p50/p99/p99.9/max 51.3/831.5/30379.2/36140.4 ms; schedule delay p99 16.2 ms; wall 136.6 s
refusals (reasons): []

writes not committed, by start (5 s windows from the isolation): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
