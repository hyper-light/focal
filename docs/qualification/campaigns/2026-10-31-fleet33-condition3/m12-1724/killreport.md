## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal5 focal10 focal11 focal12 focal13 focal20 focal21 focal22 focal24 focal28 focal31 focal32; all killed by 2.9 s, started again at 71.1 s; serving 72.4 to 74.6 s (12 of 12), 1.3 to 3.5 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 72e60650 | focal1 focal23 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1511 | 1.7 (72.3–74.1) | 77 / 5.9 |
| 1 | 76154372 | focal4 focal6 focal17 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1142 | 0.3 (2.3–2.6) | 91 / 19.9 |
| 2 | 7c829cfa | focal5* focal7 focal12* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1094 | 66.9 (1.0–67.9) | 101 / 29.9 |
| 3 | 64001b60 | focal10* focal26 focal30 | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1396 | 11.4 (57.8–69.2) | 72 / 0.9 |
| 4 | a523603c | focal2 focal3 focal13* | leader | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1064 | 1.2 (1.0–2.2) | 93 / 21.9 |
| 5 | 969874c0 | focal9 focal8 focal16 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1029 | 0.4 (2.3–2.6) | 101 / 29.9 |
| 6 | f4861de3 | focal15 focal14 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1003 | 0.3 (2.3–2.6) | 101 / 29.9 |
| 7 | 6ca88519 | focal22* focal24* focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 977 | 69.2 (1.0–70.2) | 101 / 29.9 |
| 8 | 6e2c7e70 | focal21* focal25 focal29 | a follower | 2,700 | 1,881 | 1,886 | 753 {'unavailable': 753} | 1 | 60 | 5 | 14 | 1106 | 266.9 (58.9–325.7, none after) | 96 / 24.9 |
| 9 | 741a7395 | focal11* focal28* focal27 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1437 | 70.4 (1.0–71.4) | 96 / 24.9 |
| 10 | 3195fce6 | focal18 focal20* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 961 | 72.7 (1.0–73.7) | 104 / 32.9 |

totals: started 29,700, written 28,881, judged 28,886, refused 753 {'unavailable': 753}, unknown 1, abandoned 60, undelivered 5, resent 14; judged/started 97.259%
writes (measured window): committed 290,644, refused 753, unknown 1; committed p50/p99/p99.9/max 392.0/40128.0/71058.6/74939.0 ms; schedule delay p99 13450.2 ms; wall 365.5 s
refusals (reasons): [['access: Unavailable', 753], ['lifecycle read: client: authoritative ledger service is unavailable', 60], ['client: outcome unknown for request #f#ded; retry the same identity', 1]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [  55,  60) refused/unavailable=49
  [  60,  65) refused/unavailable=100
  [  65,  70) refused/unavailable=100
  [  70,  75) refused/unavailable=100
  [  75,  80) refused/unavailable=100
  [  80,  85) refused/unavailable=100
  [  85,  90) refused/unavailable=100
  [  90,  95) refused/unavailable=99, unknown/-=1
  [  95, 100) refused/unavailable=5

not committed: 754; started 58.7 to 95.2 s; latency 119.5 to 240.5 s; steps {7: 4, 6: 2, 11: 2, 2: 3, 4: 1, 9: 1, 1: 731, 8: 2, 5: 4, 10: 2, 3: 2}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 1
  focal1: None control of 17ef3c85cd9c2bbcc33a17bd396a325d/6e2c7e7089595ed73b224525e5bc8b03: metadata owner: metadata admission capacity exceeded
sessions stopped on a node: 0
