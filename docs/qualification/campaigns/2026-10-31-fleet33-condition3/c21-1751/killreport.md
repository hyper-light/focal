## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.2 s, joined again at 12.6 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | a39eb8b3 | focal1* focal21 focal26 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 116 | 2.5 (0.1–2.6) | 93 / 80.4 |
| 1 | d6ecb963 | focal4 focal12 focal20 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 124 | 0.4 (1.0–1.3) | 94 / 81.4 |
| 2 | ca4f234d | focal7* focal27* focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 120 | 13.8 (0.1–13.9) | 93 / 80.4 |
| 3 | e464c3b2 | focal6* focal8* focal10 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 111 | 13.5 (0.2–13.6) | 93 / 80.4 |
| 4 | 4ca3c86b | focal13 focal14 focal18 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 97 | 0.5 (0.4–0.9) | 94 / 81.4 |
| 5 | ec80c58e | focal3 focal2 focal16* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 130 | 1.5 (0.2–1.6) | 92 / 79.4 |
| 6 | aadd2f5f | focal11 focal19 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 103 | 0.5 (0.6–1.1) | 94 / 81.4 |
| 7 | 8003516a | focal17* focal22 focal24 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 136 | 0.5 (1.2–1.7) | 94 / 81.4 |
| 8 | c73d79df | focal25 focal29 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 128 | 0.5 (0.6–1.1) | 93 / 80.4 |
| 9 | e4f50993 | focal5 focal9* focal28* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 154 | 12.7 (0.1–12.8) | 92 / 79.4 |
| 10 | a7bb0964 | focal15 focal23* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 132 | 13.5 (0.2–13.7) | 94 / 81.4 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 0; judged/started 100.000%
writes (measured window): committed 291,395, refused 0, unknown 0; committed p50/p99/p99.9/max 60.3/511.9/10537.2/14894.2 ms; schedule delay p99 4.6 ms; wall 136.7 s
refusals (reasons): []

writes not committed, by start (5 s windows from the isolation): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
