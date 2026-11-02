## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal2 focal9 focal11 focal12 focal13 focal20 focal21 focal22 focal24 focal28 focal31 focal32; all killed by 1.4 s, started again at 69.0 s; serving 69.4 to 71.9 s (12 of 12), 0.4 to 2.9 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 5d8ece75 | focal1 focal26 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 414 | 0.4 (93.7–94.1) | 94 / 25.0 |
| 1 | 05debf56 | focal4 focal32* focal33 | a follower | 2,700 | 2,394 | 2,394 | 306 {'unavailable': 306} | 0 | 0 | 0 | 159 | 358 | 5.2 (46.7–51.9) | 94 / 25.0 |
| 2 | 71c4614f | focal7 focal8 focal24* | a follower | 2,700 | 2,273 | 2,273 | 427 {'unavailable': 427} | 0 | 0 | 0 | 149 | 400 | 19.6 (37.6–57.2) | 94 / 25.0 |
| 3 | fec905bc | focal10 focal15 focal23 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 446 | 0.6 (67.3–67.9) | 84 / 15.0 |
| 4 | 859b7e3c | focal2* focal3 focal13* | quorum | 2,700 | 2,153 | 2,153 | 547 {'owner_bound': 1, 'unavailable': 546} | 0 | 0 | 0 | 419 | 415 | 70.2 (0.2–70.5) | 94 / 25.0 |
| 5 | 954f4f47 | focal5 focal12* focal16 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 364 | 8.1 (93.1–101.2) | 95 / 26.0 |
| 6 | bab249da | focal9* focal14 focal19 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 379 | 0.4 (67.3–67.7) | 94 / 25.0 |
| 7 | f810d053 | focal21* focal20* focal22* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 984 | 336 | 69.2 (0.2–69.4) | 94 / 25.0 |
| 8 | 67ce0ad3 | focal25 focal27 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 362 | 0.4 (39.8–40.2) | 94 / 25.0 |
| 9 | bacb919d | focal11* focal18 focal28* | quorum | 2,700 | 2,219 | 2,219 | 481 {'owner_bound': 2, 'unavailable': 479} | 0 | 0 | 0 | 379 | 339 | 66.4 (0.3–66.7) | 94 / 25.0 |
| 10 | a16566df | focal6 focal17 focal31* | leader | 2,700 | 1,978 | 1,978 | 722 {'owner_bound': 19, 'unavailable': 703} | 0 | 0 | 0 | 241 | 374 | 15.9 (90.5–106.3) | 95 / 26.0 |

totals: started 29,700, written 27,217, judged 27,217, refused 2,483 {'owner_bound': 22, 'unavailable': 2461}, unknown 0, abandoned 0, undelivered 0, resent 2331; judged/started 91.640%
writes (measured window): committed 270,582, refused 2,483, unknown 0; committed p50/p99/p99.9/max 160.0/24319.0/62467.0/74348.3 ms; schedule delay p99 38.3 ms; wall 162.6 s
refusals (reasons): [['access: Unavailable', 2461], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 20], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 2]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=10, refused/unavailable=232
  [   5,  10) refused/unavailable=282
  [  10,  15) refused/owner_bound=12, refused/unavailable=505
  [  15,  20) refused/unavailable=382
  [  20,  25) refused/unavailable=347
  [  25,  30) refused/unavailable=285
  [  30,  35) refused/unavailable=206
  [  35,  40) refused/unavailable=169
  [  40,  45) refused/unavailable=53

not committed: 2483; started 0.2 to 44.0 s; latency 0.0 to 60.6 s; steps {4: 1412, 10: 26, 9: 22, 1: 1011, 6: 1, 8: 3, 7: 3, 3: 3, 2: 2}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 1
  focal1: None control of 342ffe483878be910a7dda5116da9816/bab249da15742b41cf9ebb2c670b8e9a: metadata owner: metadata owner is unavailable
sessions stopped on a node: 0
