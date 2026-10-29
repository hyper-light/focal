## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.2 s, started again at 34.0 s; serving 34.3 to 36.8 s (12 of 12), 0.3 to 2.8 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | f20c422c | focal1* focal3 focal26 | leader | 2,700 | 2,698 | 2,698 | 2 {'owner_bound': 2} | 0 | 0 | 0 | 1 | 152 | 10.0 | 65 / 31.0 |
| 1 | 72b25c0c | focal2 focal4 focal6* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 143 | 5.5 | 65 / 31.0 |
| 2 | 48b8b757 | focal7* focal24 focal32* | quorum | 2,700 | 2,698 | 2,698 | 2 {'owner_bound': 2} | 0 | 0 | 0 | 126 | 136 | 35.8 | 77 / 43.0 |
| 3 | 0e91092a | focal5 focal10 focal18 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 164 | 0.5 | 51 / 17.0 |
| 4 | c6f8839f | focal9* focal13 focal29 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 182 | 6.7 | 51 / 17.0 |
| 5 | 15093814 | focal16* focal23* focal30 | quorum | 2,700 | 2,698 | 2,698 | 2 {'owner_bound': 2} | 0 | 0 | 0 | 166 | 154 | 36.9 | 65 / 31.0 |
| 6 | d71fa863 | focal12 focal19 focal20 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 134 | 0.5 | 90 / 56.0 |
| 7 | 4aec0a90 | focal8* focal22 focal33 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 147 | 2.8 | 72 / 38.0 |
| 8 | 5da4f547 | focal17* focal25 focal27* | quorum | 2,700 | 2,608 | 2,608 | 92 {'unavailable': 92} | 0 | 0 | 0 | 12 | 132 | 34.8 | 68 / 34.0 |
| 9 | ce4858f4 | focal14 focal15 focal28* | leader | 2,700 | 2,697 | 2,697 | 3 {'owner_bound': 3} | 0 | 0 | 0 | 112 | 155 | 35.2 | 65 / 31.0 |
| 10 | 3f651203 | focal11 focal21 focal31* | leader | 2,700 | 2,696 | 2,696 | 4 {'owner_bound': 4} | 0 | 0 | 0 | 4 | 155 | 10.5 | 65 / 31.0 |

totals: started 29,700, written 29,595, judged 29,595, refused 105 {'owner_bound': 13, 'unavailable': 92}, unknown 0, abandoned 0, undelivered 0, resent 421; judged/started 99.646%
writes (measured window): committed 290,407, refused 105, unknown 0; committed p50/p99/p99.9/max 42.8/15339.1/32113.0/38593.9 ms; schedule delay p99 10891.3 ms; wall 135.8 s
refusals (reasons): [['access: Unavailable', 92], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 13]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=5, refused/unavailable=90
  [   5,  10) refused/unavailable=2
  [  10,  15) refused/owner_bound=5
  [  20,  25) refused/owner_bound=3

not committed: 105; started 0.2 to 24.0 s; latency 9.2 to 30.0 s; steps {9: 13, 1: 92}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
