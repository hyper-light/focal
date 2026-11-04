## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal4 focal9 focal10 focal11 focal12 focal19 focal20 focal23 focal27 focal30 focal31 focal33; all killed by 2.4 s, started again at 70.8 s; serving 71.9 to 73.8 s (12 of 12), 1.1 to 3.0 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 316c5f8e | focal1 focal2 focal3 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1777 | 0.5 (63.7–64.2) | 78 / 7.2 |
| 1 | 66312817 | focal4* focal12* focal20* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 75302 | 71.4 (1.0–72.4) | 0 / -70.8 |
| 2 | cb7caff5 | focal7 focal23* focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 72915 | 70.1 (1.0–71.1) | 1 / -69.8 |
| 3 | 2403d6ac | focal8 focal10* focal15 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 6618 | 13.0 (47.1–60.1) | 55 / -15.8 |
| 4 | a97c33e6 | focal5 focal13 focal33* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1706 | 0.4 (94.6–95.0) | 78 / 7.2 |
| 5 | 89898098 | focal11* focal16 focal21 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1740 | 0.4 (94.6–95.0) | 78 / 7.2 |
| 6 | 3f549a1c | focal6 focal14 focal19* | leader | 2,700 | 1,971 | 1,979 | 618 {'unavailable': 618} | 0 | 101 | 10 | 71 | 6622 | 253.5 (64.6–318.1, none after) | 93 / 22.2 |
| 7 | e793ed44 | focal22 focal24 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1501 | 1.7 (52.0–53.7) | 96 / 25.2 |
| 8 | eefea806 | focal25 focal32 focal30* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1571 | 0.5 (5.0–5.5) | 79 / 8.2 |
| 9 | 56b60978 | focal17 focal18 focal28 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1757 | 0.3 (94.6–95.0) | 77 / 6.2 |
| 10 | afcf7b2c | focal9* focal29 focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 81811 | 75.6 (0.7–76.3) | 0 / -70.8 |

totals: started 29,700, written 28,971, judged 28,979, refused 618 {'unavailable': 618}, unknown 0, abandoned 101, undelivered 10, resent 71; judged/started 97.572%
writes (measured window): committed 289,073, refused 618, unknown 0; committed p50/p99/p99.9/max 453.4/43615.1/76684.5/81810.6 ms; schedule delay p99 14982.3 ms; wall 480.6 s
refusals (reasons): [['access: Unavailable', 618], ['lifecycle read: client: authoritative ledger service is unavailable', 101], ['floor advance: access: Unavailable', 36]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [  60,  65) refused/unavailable=76
  [  65,  70) refused/unavailable=100
  [  70,  75) refused/unavailable=100
  [  75,  80) refused/unavailable=100
  [  80,  85) refused/unavailable=100
  [  85,  90) refused/unavailable=100
  [  90,  95) refused/unavailable=42

not committed: 618; started 64.0 to 92.1 s; latency 119.5 to 240.6 s; steps {1: 562, 8: 4, 2: 6, 5: 8, 10: 5, 9: 6, 4: 6, 3: 8, 11: 5, 7: 5, 6: 3}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 1
  focal1: None control of a8492a79a7a558ca11af6291383d7988/3f549a1c7556107dc0c774fe889c0ba2: metadata owner: metadata admission capacity exceeded
sessions stopped on a node: 0
