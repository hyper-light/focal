## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 15.9 s, started again at 49.8 s; serving 50.3 to 53.1 s (12 of 12), 0.5 to 3.4 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | a9645d7b | focal1* focal15 focal20 | leader | 2,700 | 2,567 | 2,567 | 133 {'owner_bound': 95, 'unavailable': 38} | 0 | 0 | 0 | 546 | 9478 | 2.7 (2.2–5.0) | 60 / 10.2 |
| 1 | f65ed632 | focal4 focal6* focal11 | a follower | 2,700 | 2,638 | 2,638 | 62 {'unavailable': 62} | 0 | 0 | 0 | 479 | 7245 | 3.4 (1.4–4.8) | 62 / 12.2 |
| 2 | 546df24d | focal7* focal27* focal29 | quorum | 2,700 | 2,584 | 2,584 | 116 {'owner_bound': 51, 'unavailable': 65} | 0 | 0 | 0 | 493 | 7362 | 52.5 (0.2–52.7) | 65 / 15.2 |
| 3 | b0f95a46 | focal10 focal12 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 7165 | 2.0 (2.2–4.2) | 0 / -49.8 |
| 4 | 5ee52315 | focal5 focal13 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 5493 | 1.9 (2.2–4.1) | 0 / -49.8 |
| 5 | 639b7074 | focal3 focal14 focal16* | leader | 2,700 | 2,563 | 2,563 | 137 {'owner_bound': 88, 'unavailable': 49} | 0 | 0 | 0 | 421 | 5640 | 2.0 (0.3–2.3) | 64 / 14.2 |
| 6 | a049a4ef | focal19 focal23* focal24 | a follower | 2,700 | 2,631 | 2,631 | 69 {'unavailable': 69} | 0 | 0 | 0 | 591 | 6964 | 3.6 (2.2–5.8) | 63 / 13.2 |
| 7 | 8f217b6c | focal9* focal17* focal22 | quorum | 2,700 | 2,670 | 2,670 | 30 {'unavailable': 30} | 0 | 0 | 0 | 518 | 5680 | 52.9 (0.3–53.2) | 68 / 18.2 |
| 8 | c86e82bd | focal2 focal18 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 6035 | 4.5 (2.2–6.7) | 3 / -46.8 |
| 9 | 574708d0 | focal28* focal32* focal33 | quorum | 2,700 | 2,672 | 2,672 | 28 {'unavailable': 28} | 0 | 0 | 0 | 567 | 9926 | 52.3 (0.3–52.6) | 63 / 13.2 |
| 10 | af98f39e | focal8* focal21 focal31* | quorum | 2,700 | 2,677 | 2,677 | 23 {'unavailable': 23} | 0 | 0 | 0 | 622 | 59212 | 51.0 (0.3–51.3) | 21 / -28.8 |

totals: started 29,700, written 29,102, judged 29,102, refused 598 {'owner_bound': 234, 'unavailable': 364}, unknown 0, abandoned 0, undelivered 0, resent 4237; judged/started 97.987%
writes (measured window): committed 287,559, refused 598, unknown 0; committed p50/p99/p99.9/max 319.1/40423.8/54090.9/59211.9 ms; schedule delay p99 7526.0 ms; wall 137.8 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 183], ['access: Unavailable', 364], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 51]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=120, refused/unavailable=28
  [   5,  10) refused/owner_bound=56, refused/unavailable=112
  [  10,  15) refused/owner_bound=58, refused/unavailable=157
  [  15,  20) refused/unavailable=49
  [  20,  25) refused/unavailable=18

not committed: 598; started 0.0 to 22.6 s; latency 0.0 to 30.1 s; steps {9: 234, 10: 44, 4: 174, 1: 146}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
