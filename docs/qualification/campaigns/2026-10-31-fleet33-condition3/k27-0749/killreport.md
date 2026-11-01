## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 11.0 s, started again at 72.5 s; serving 73.1 to 76.1 s (12 of 12), 0.6 to 3.5 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 64eee804 | focal1* focal15 focal29 | leader | 2,700 | 2,458 | 2,458 | 240 {'owner_bound': 53, 'unavailable': 187} | 0 | 2 | 0 | 724 | 804 | 7.4 (77.8–85.2) | 136 / 63.5 |
| 1 | bfcc43e4 | focal4 focal23* focal30 | a follower | 2,700 | 2,519 | 2,519 | 181 {'unavailable': 181} | 0 | 0 | 0 | 391 | 604 | 5.1 (77.9–83.0) | 124 / 51.5 |
| 2 | 71dba554 | focal7* focal8* focal21 | quorum | 2,700 | 2,585 | 2,585 | 115 {'owner_bound': 50, 'unavailable': 65} | 0 | 0 | 0 | 456 | 710 | 53.1 (0.3–53.4) | 133 / 60.5 |
| 3 | ece4bfe3 | focal10 focal32* focal33 | a follower | 2,700 | 2,549 | 2,549 | 151 {'unavailable': 151} | 0 | 0 | 0 | 247 | 655 | 5.1 (78.0–83.1) | 123 / 50.5 |
| 4 | 00466c96 | focal9* focal13 focal20 | a follower | 2,700 | 2,505 | 2,505 | 195 {'owner_bound': 7, 'unavailable': 188} | 0 | 0 | 0 | 381 | 740 | 5.1 (77.9–83.1) | 123 / 50.5 |
| 5 | e834a083 | focal12 focal16* focal17* | quorum | 2,700 | 2,589 | 2,589 | 111 {'owner_bound': 5, 'unavailable': 106} | 0 | 0 | 0 | 455 | 718 | 52.5 (0.3–52.8) | 131 / 58.5 |
| 6 | a23c6867 | focal14 focal19 focal24 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 708 | 5.0 (78.0–83.0) | 123 / 50.5 |
| 7 | 5bf93579 | focal5 focal6* focal22 | a follower | 2,700 | 2,427 | 2,427 | 272 {'unavailable': 272} | 1 | 0 | 0 | 450 | 747 | 22.2 (41.8–64.0) | 134 / 61.5 |
| 8 | dc55354e | focal25 focal26 focal27* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 721 | 5.5 (77.9–83.4) | 126 / 53.5 |
| 9 | 618e9188 | focal11 focal18 focal28* | leader | 2,700 | 2,477 | 2,477 | 223 {'owner_bound': 51, 'unavailable': 172} | 0 | 0 | 0 | 379 | 649 | 6.9 (77.9–84.9) | 126 / 53.5 |
| 10 | f26a0fe6 | focal2 focal3 focal31* | leader | 2,700 | 2,445 | 2,445 | 254 {'owner_bound': 53, 'unavailable': 201} | 0 | 1 | 0 | 800 | 675 | 7.1 (78.0–85.0) | 134 / 61.5 |

totals: started 29,700, written 27,954, judged 27,954, refused 1,742 {'owner_bound': 219, 'unavailable': 1523}, unknown 1, abandoned 3, undelivered 0, resent 4283; judged/started 94.121%
writes (measured window): committed 281,236, refused 1,742, unknown 1; committed p50/p99/p99.9/max 905.7/46926.5/72178.1/88623.9 ms; schedule delay p99 27082.7 ms; wall 180.0 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 214], ['access: Unavailable', 1523], ['lifecycle read: client: serving replica is behind; published sequence is #', 3], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 5], ['client: invalid server response', 1]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=37, refused/unavailable=36
  [   5,  10) refused/owner_bound=64, refused/unavailable=321
  [  10,  15) refused/owner_bound=56, refused/unavailable=505
  [  15,  20) refused/owner_bound=12, refused/unavailable=352
  [  20,  25) refused/unavailable=200
  [  25,  30) refused/unavailable=39
  [  30,  35) refused/unavailable=39
  [  35,  40) refused/unavailable=10
  [  40,  45) refused/unavailable=14
  [  45,  50) refused/unavailable=5
  [  50,  55) refused/owner_bound=1
  [  55,  60) refused/owner_bound=17, refused/unavailable=2
  [  60,  65) refused/owner_bound=19, unknown/-=1
  [  65,  70) refused/owner_bound=13

not committed: 1743; started 0.2 to 68.6 s; latency 0.0 to 61.2 s; steps {9: 219, 4: 1265, 10: 87, 1: 172}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
