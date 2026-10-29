## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.2 s, started again at 34.2 s; serving 34.5 to 36.1 s (12 of 12), 0.4 to 1.9 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0b426369 | focal1* focal18 focal26 | leader | 2,700 | 2,694 | 2,694 | 6 {'owner_bound': 6} | 0 | 0 | 0 | 56 | 194 | 10.1 | 66 / 31.8 |
| 1 | 629e1f0b | focal2 focal4 focal27* | a follower | 2,700 | 2,650 | 2,650 | 50 {'unavailable': 50} | 0 | 0 | 0 | 10 | 227 | 6.4 | 52 / 17.8 |
| 2 | 37919411 | focal7* focal17* focal24 | quorum | 2,700 | 2,698 | 2,698 | 2 {'owner_bound': 2} | 0 | 0 | 0 | 177 | 153 | 36.3 | 74 / 39.8 |
| 3 | 4fc6882c | focal10 focal20 focal21 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 145 | 0.5 | 66 / 31.8 |
| 4 | 11e5ae67 | focal3 focal5 focal13 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 155 | 0.6 | 66 / 31.8 |
| 5 | 32d48167 | focal9* focal16* focal29 | quorum | 2,700 | 2,622 | 2,622 | 78 {'owner_bound': 1, 'unavailable': 77} | 0 | 0 | 0 | 167 | 177 | 34.7 | 66 / 31.8 |
| 6 | 4b33b203 | focal14 focal19 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 155 | 0.5 | 66 / 31.8 |
| 7 | 4b921691 | focal12 focal22 focal23* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 177 | 3.4 | 66 / 31.8 |
| 8 | 5cc54e77 | focal15 focal25 focal32* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 148 | 3.9 | 66 / 31.8 |
| 9 | 27c3f743 | focal11 focal28* focal30 | leader | 2,700 | 2,698 | 2,698 | 2 {'owner_bound': 2} | 0 | 0 | 0 | 2 | 192 | 10.5 | 66 / 31.8 |
| 10 | 6515a5e1 | focal6* focal8* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 268 | 156 | 36.7 | 66 / 31.8 |

totals: started 29,700, written 29,562, judged 29,562, refused 138 {'owner_bound': 11, 'unavailable': 127}, unknown 0, abandoned 0, undelivered 0, resent 680; judged/started 99.535%
writes (measured window): committed 290,357, refused 138, unknown 0; committed p50/p99/p99.9/max 34.5/14299.1/31786.5/38450.7 ms; schedule delay p99 8967.4 ms; wall 135.7 s
refusals (reasons): [['access: Unavailable', 127], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 10], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 1]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=4, refused/unavailable=36
  [   5,  10) refused/unavailable=14
  [  10,  15) refused/owner_bound=6
  [  30,  35) refused/owner_bound=1, refused/unavailable=2
  [  35,  40) refused/unavailable=75

not committed: 138; started 0.2 to 36.6 s; latency 0.0 to 30.0 s; steps {9: 11, 4: 117, 10: 10}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 1
  focal4: None metadata owner: metadata owner is unavailable
