## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal2 focal10 focal11 focal12 focal13 focal20 focal21 focal22 focal24 focal28 focal31 focal32; all killed by 11.2 s, started again at 47.7 s; serving 48.2 to 50.1 s (12 of 12), 0.5 to 2.4 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 23a5113c | focal1 focal18 focal23 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 347 | 1.5 (9.4–10.9) | 71 / 23.3 |
| 1 | 85b08e98 | focal4 focal8 focal21* | a follower | 2,700 | 2,371 | 2,371 | 329 {'unavailable': 329} | 0 | 0 | 0 | 119 | 377 | 1.5 (9.5–10.9) | 76 / 28.3 |
| 2 | 8faffafe | focal7 focal12* focal14 | a follower | 2,700 | 2,484 | 2,484 | 216 {'unavailable': 216} | 0 | 0 | 0 | 123 | 322 | 1.4 (9.5–10.9) | 76 / 28.3 |
| 3 | cc8d0296 | focal3 focal10* focal29 | leader | 2,700 | 2,410 | 2,410 | 290 {'owner_bound': 19, 'unavailable': 271} | 0 | 0 | 0 | 111 | 409 | 3.1 (9.5–12.6) | 66 / 18.3 |
| 4 | 0002d6e3 | focal9 focal13* focal20* | quorum | 2,700 | 2,592 | 2,592 | 108 {'owner_bound': 3, 'unavailable': 105} | 0 | 0 | 0 | 317 | 394 | 48.9 (0.2–49.2) | 66 / 18.3 |
| 5 | a829d5e6 | focal16 focal17 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 353 | 3.0 (9.4–12.4) | 70 / 22.3 |
| 6 | d9b336b6 | focal5 focal6 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 381 | 1.5 (9.5–10.9) | 75 / 27.3 |
| 7 | 0c30bae2 | focal11* focal22* focal24* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 578 | 360 | 48.2 (0.2–48.4) | 85 / 37.3 |
| 8 | 9c8eaac0 | focal15 focal25 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 330 | 1.4 (9.5–10.9) | 95 / 47.3 |
| 9 | d09b5e9f | focal28* focal32* focal33 | quorum | 2,700 | 2,590 | 2,590 | 110 {'owner_bound': 1, 'unavailable': 109} | 0 | 0 | 0 | 315 | 281 | 49.5 (0.2–49.7) | 71 / 23.3 |
| 10 | d858eed5 | focal2* focal27 focal31* | quorum | 2,700 | 2,648 | 2,648 | 52 {'owner_bound': 1, 'unavailable': 51} | 0 | 0 | 0 | 288 | 395 | 45.2 (0.2–45.5) | 65 / 17.3 |

totals: started 29,700, written 28,595, judged 28,595, refused 1,105 {'owner_bound': 24, 'unavailable': 1081}, unknown 0, abandoned 0, undelivered 0, resent 1851; judged/started 96.279%
writes (measured window): committed 282,329, refused 1,105, unknown 0; committed p50/p99/p99.9/max 152.4/22192.5/47105.6/52928.4 ms; schedule delay p99 45.8 ms; wall 137.9 s
refusals (reasons): [['access: Unavailable', 1081], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 23], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 1]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=9, refused/unavailable=63
  [   5,  10) refused/owner_bound=2, refused/unavailable=153
  [  10,  15) refused/owner_bound=12, refused/unavailable=623
  [  15,  20) refused/unavailable=229
  [  20,  25) refused/unavailable=13
  [  45,  50) refused/owner_bound=1

not committed: 1105; started 0.1 to 49.7 s; latency 0.0 to 30.2 s; steps {4: 784, 10: 32, 1: 265, 9: 24}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
