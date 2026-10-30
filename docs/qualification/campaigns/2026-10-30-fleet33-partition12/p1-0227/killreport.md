## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 30.4 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | d699eb89 | focal1* focal17* focal30 | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 38 | 166 | 30.6 (0.1–30.8) | 78 / 47.6 |
| 1 | 70413a44 | focal4 focal14 focal24 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 149 | 0.5 (0.5–1.0) | 92 / 61.6 |
| 2 | f920c753 | focal7* focal15 focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 38 | 171 | 30.9 (0.1–31.0) | 78 / 47.6 |
| 3 | 39c7f213 | focal6* focal10 focal26 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 137 | 0.5 (0.7–1.2) | 82 / 51.6 |
| 4 | 6d369075 | focal9* focal11 focal13 | a follower | 2,700 | 2,699 | 2,699 | 1 {'unavailable': 1} | 0 | 0 | 0 | 6 | 195 | 15.3 (15.4–30.7) | 78 / 47.6 |
| 5 | 1e36bb83 | focal16* focal29 focal33 | leader | 2,700 | 2,689 | 2,689 | 11 {'owner_bound': 10, 'ingress_capacity': 1} | 0 | 0 | 0 | 12 | 153 | 10.1 (0.1–10.2) | 78 / 47.6 |
| 6 | 40da3c2d | focal5 focal19 focal21 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 111 | 0.5 (0.6–1.1) | 92 / 61.6 |
| 7 | 48ffbe3a | focal18 focal22 focal23* | a follower | 2,700 | 2,697 | 2,697 | 3 {'unavailable': 3} | 0 | 0 | 0 | 8 | 172 | 14.8 (15.4–30.1) | 78 / 47.6 |
| 8 | 2e260d5f | focal8* focal12 focal25 | a follower | 2,700 | 2,695 | 2,695 | 5 {'unavailable': 5} | 0 | 0 | 0 | 6 | 130 | 14.9 (15.3–30.2) | 79 / 48.6 |
| 9 | e54cfffe | focal2 focal3 focal28* | leader | 2,700 | 2,689 | 2,689 | 11 {'owner_bound': 10, 'ingress_capacity': 1} | 0 | 0 | 0 | 2 | 129 | 10.1 (0.1–10.2) | 78 / 47.6 |
| 10 | 81980f30 | focal20 focal27* focal31* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 97 | 154 | 34.0 (0.1–34.1) | 78 / 47.6 |

totals: started 29,700, written 29,668, judged 29,668, refused 32 {'owner_bound': 21, 'ingress_capacity': 2, 'unavailable': 9}, unknown 0, abandoned 0, undelivered 0, resent 207; judged/started 99.892%
writes (measured window): committed 291,583, refused 32, unknown 0; committed p50/p99/p99.9/max 47.3/16735.3/28566.2/35102.5 ms; schedule delay p99 5418.0 ms; wall 136.0 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 20], ['access: Unavailable', 9], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 1], ['access: Capacity', 2]]

writes not committed, by start (5 s windows from the isolation): outcome/cause=count
  [   0,   5) refused/owner_bound=1, refused/unavailable=9
  [  10,  15) refused/ingress_capacity=2, refused/owner_bound=20

not committed: 32; started 0.1 to 12.5 s; latency 6.0 to 30.0 s; steps {9: 21, 4: 8, 10: 3}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
