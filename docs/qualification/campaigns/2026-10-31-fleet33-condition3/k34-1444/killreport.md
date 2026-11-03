## focal-220-128-work-agents500-nodes33-retry-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.2 s, started again at 34.2 s; serving 34.6 to 40.5 s (12 of 12), 0.4 to 6.4 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 70c5c53a | focal1* focal2 focal3 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 72 | 11939 | 1.3 (0.0–1.3) | 25 / -9.2 |
| 1 | 76dddc4c | focal4 focal9* focal29 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 38 | 881 | 0.5 (0.0–0.5) | 35 / 0.8 |
| 2 | d1e26d3a | focal7* focal23* focal24 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 109 | 35290 | 34.3 (0.0–34.3) | 0 / -34.2 |
| 3 | cea6215e | focal10 focal11 focal21 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 937 | 0.5 (0.7–1.2) | 0 / -34.2 |
| 4 | 60412dbd | focal6* focal13 focal20 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 98 | 738 | 0.5 (1.0–1.5) | 45 / 10.8 |
| 5 | 91354d7b | focal14 focal15 focal16* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 46 | 12123 | 1.8 (0.0–1.8) | 26 / -8.2 |
| 6 | ef31115f | focal19 focal27* focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 98 | 38509 | 36.5 (0.0–36.5) | 0 / -34.2 |
| 7 | 2ff57ccd | focal5 focal12 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 897 | 0.5 (1.1–1.6) | 0 / -34.2 |
| 8 | 4e5f478e | focal25 focal26 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 706 | 0.5 (1.1–1.6) | 0 / -34.2 |
| 9 | 262abad3 | focal18 focal17* focal28* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 149 | 37675 | 35.8 (0.2–36.1) | 0 / -34.2 |
| 10 | 16783d4e | focal8* focal31* focal33 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 125 | 38117 | 35.2 (0.0–35.2) | 0 / -34.2 |

totals: started 29,700, written 29,700, judged 29,700, refused 0 {}, unknown 0, abandoned 0, undelivered 0, resent 735; judged/started 100.000%
writes (measured window): committed 291,695, refused 0, unknown 0; committed p50/p99/p99.9/max 36.0/18985.9/34218.3/38523.7 ms; schedule delay p99 20.3 ms; wall 135.7 s
refusals (reasons): []

writes not committed, by start (5 s windows from the kill): outcome/cause=count

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
