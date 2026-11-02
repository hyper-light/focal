## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 2.0 s, started again at 38.6 s; serving 39.1 to 44.1 s (12 of 12), 0.5 to 5.5 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 6f23ccab | focal1* focal8* focal15 | quorum | 2,700 | 2,693 | 2,693 | 7 {'owner_bound': 7} | 0 | 0 | 0 | 171 | 177 | 37.4 (0.2–37.7) | 94 / 55.4 |
| 1 | d703bf4a | focal3 focal4 focal5 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 181 | 0.5 (65.0–65.6) | 94 / 55.4 |
| 2 | 60f00a77 | focal7* focal24 focal29 | leader | 2,700 | 2,653 | 2,653 | 47 {'owner_bound': 13, 'unavailable': 34} | 0 | 0 | 0 | 25 | 164 | 1.5 (0.2–1.7) | 94 / 55.4 |
| 3 | 4a7b4060 | focal10 focal17* focal21 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 173 | 0.6 (0.9–1.5) | 94 / 55.4 |
| 4 | 26051805 | focal13 focal18 focal23* | a follower | 2,700 | 2,609 | 2,609 | 91 {'unavailable': 91} | 0 | 0 | 0 | 101 | 207 | 7.9 (77.6–85.5) | 94 / 55.4 |
| 5 | ccb8002e | focal2 focal16* focal33 | leader | 2,700 | 2,633 | 2,633 | 67 {'owner_bound': 12, 'ingress_capacity': 49, 'unavailable': 6} | 0 | 0 | 0 | 128 | 218 | 2.7 (16.0–18.7) | 87 / 48.4 |
| 6 | 3cb0176b | focal9* focal14 focal19 | a follower | 2,700 | 2,612 | 2,612 | 88 {'unavailable': 88} | 0 | 0 | 0 | 63 | 169 | 12.9 (79.7–92.6) | 99 / 60.4 |
| 7 | 6b7b2b57 | focal12 focal20 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 190 | 0.7 (0.7–1.4) | 94 / 55.4 |
| 8 | 0ee10e8d | focal6* focal11 focal25 | a follower | 2,700 | 2,572 | 2,572 | 128 {'unavailable': 128} | 0 | 0 | 0 | 145 | 172 | 8.7 (64.1–72.8) | 94 / 55.4 |
| 9 | 937e4697 | focal28* focal30 focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 235 | 183 | 41.2 (0.2–41.4) | 90 / 51.4 |
| 10 | b64cf965 | focal26 focal27* focal31* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 213 | 170 | 39.8 (0.2–40.0) | 90 / 51.4 |

totals: started 29,700, written 29,271, judged 29,271, refused 429 {'owner_bound': 33, 'ingress_capacity': 49, 'unavailable': 347}, unknown 0, abandoned 0, undelivered 0, resent 1081; judged/started 98.556%
writes (measured window): committed 288,162, refused 429, unknown 0; committed p50/p99/p99.9/max 143.6/20027.0/37205.3/43711.6 ms; schedule delay p99 41.1 ms; wall 167.0 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 30], ['access: Unavailable', 347], ['access: Capacity', 49], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 3]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=15, refused/unavailable=102
  [   5,  10) refused/owner_bound=1, refused/unavailable=178
  [  10,  15) refused/ingress_capacity=2, refused/owner_bound=10, refused/unavailable=66
  [  15,  20) refused/ingress_capacity=47
  [  35,  40) refused/owner_bound=3
  [  40,  45) refused/owner_bound=4
  [  95, 100) refused/unavailable=1

not committed: 429; started 0.2 to 95.6 s; latency 0.0 to 30.2 s; steps {9: 33, 4: 333, 10: 13, 3: 2, 1: 48}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 1
  focal5: None control of 7e245f7e7998b686d6f2688930901633/60f00a7743eb688d7fe68788063b5f93: metadata owner: metadata outcome is unknown; retry the same identity
sessions stopped on a node: 0
