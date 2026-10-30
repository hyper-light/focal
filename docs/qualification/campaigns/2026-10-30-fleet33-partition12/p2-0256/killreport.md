## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 30.4 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 860077d9 | focal1* focal32* focal33 | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 43 | 170 | 31.9 (0.1–32.0) | 72 / 41.6 |
| 1 | 76b2f93a | focal4 focal24 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 147 | 0.5 (0.3–0.8) | 69 / 38.6 |
| 2 | 7824f596 | focal5 focal7* focal30 | leader | 2,700 | 2,688 | 2,688 | 12 {'owner_bound': 10, 'ingress_capacity': 2} | 0 | 0 | 0 | 2 | 148 | 11.4 (20.5–31.8) | 47 / 16.6 |
| 3 | 9d6fdde0 | focal6* focal10 focal8* | quorum | 2,700 | 2,689 | 2,689 | 11 {'unavailable': 11} | 0 | 0 | 0 | 6 | 101 | 30.5 (0.1–30.6) | 78 / 47.6 |
| 4 | f6020925 | focal15 focal13 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 152 | 0.5 (0.5–1.0) | 46 / 15.6 |
| 5 | fcf46939 | focal2 focal16* focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 81 | 153 | 33.2 (0.1–33.3) | 52 / 21.6 |
| 6 | e92f6fae | focal9* focal20 focal19 | a follower | 2,700 | 2,692 | 2,692 | 8 {'unavailable': 8} | 0 | 0 | 0 | 0 | 131 | 14.5 (15.7–30.1) | 78 / 47.6 |
| 7 | c23f1939 | focal11 focal18 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 143 | 0.5 (0.3–0.8) | 91 / 60.6 |
| 8 | c145bcfc | focal3 focal17* focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 170 | 0.5 (1.0–1.5) | 46 / 15.6 |
| 9 | cf5fb5dd | focal12 focal23* focal28* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 43 | 137 | 31.8 (0.1–31.9) | 47 / 16.6 |
| 10 | a67ee6a5 | focal14 focal21 focal31* | leader | 2,700 | 2,688 | 2,688 | 12 {'owner_bound': 10, 'ingress_capacity': 2} | 0 | 0 | 0 | 2 | 162 | 13.7 (20.5–34.2) | 47 / 16.6 |

totals: started 29,700, written 29,656, judged 29,656, refused 44 {'owner_bound': 21, 'ingress_capacity': 4, 'unavailable': 19}, unknown 0, abandoned 0, undelivered 0, resent 177; judged/started 99.852%
writes (measured window): committed 291,388, refused 44, unknown 0; committed p50/p99/p99.9/max 44.8/14954.4/28661.4/34642.7 ms; schedule delay p99 4458.4 ms; wall 135.8 s
refusals (reasons): [['access: Capacity', 4], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 21], ['access: Unavailable', 19]]

writes not committed, by start (5 s windows from the isolation): outcome/cause=count
  [   0,   5) refused/owner_bound=4, refused/unavailable=19
  [  10,  15) refused/ingress_capacity=4, refused/owner_bound=17

not committed: 44; started 0.1 to 13.4 s; latency 5.0 to 30.0 s; steps {9: 21, 1: 11, 4: 8, 10: 4}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
