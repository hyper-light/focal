## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 2.4 s, started again at 37.9 s; serving 38.2 to 39.6 s (12 of 12), 0.3 to 1.7 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 626386cc | focal1* focal15 focal29 | leader | 2,700 | 2,641 | 2,641 | 59 {'owner_bound': 29, 'unavailable': 30} | 0 | 0 | 0 | 131 | 197 | 2.4 (0.2–2.6) | 95 / 57.1 |
| 1 | 5215bfb6 | focal4 focal14 focal24 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 229 | 0.5 (1.2–1.7) | 80 / 42.1 |
| 2 | 33b24f3f | focal7* focal9* focal23* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 383 | 281 | 38.9 (0.2–39.1) | 71 / 33.1 |
| 3 | a3118c14 | focal2 focal3 focal10 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 242 | 0.5 (0.5–1.0) | 73 / 35.1 |
| 4 | 56dcf88b | focal13 focal20 focal21 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 204 | 0.4 (0.9–1.3) | 93 / 55.1 |
| 5 | 0dc1c874 | focal5 focal6* focal16* | quorum | 2,700 | 2,695 | 2,695 | 5 {'owner_bound': 3, 'unavailable': 2} | 0 | 0 | 0 | 219 | 229 | 39.1 (0.2–39.3) | 71 / 33.1 |
| 6 | 5799e5fd | focal19 focal26 focal27* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 300 | 0.4 (0.8–1.2) | 95 / 57.1 |
| 7 | 8175f830 | focal22 focal32* focal33 | a follower | 2,700 | 2,665 | 2,665 | 35 {'unavailable': 35} | 0 | 0 | 0 | 184 | 209 | 0.5 (0.3–0.8) | 74 / 36.1 |
| 8 | 932a15b1 | focal11 focal25 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 227 | 0.4 (0.5–0.9) | 73 / 35.1 |
| 9 | 1ab65595 | focal8* focal12 focal28* | quorum | 2,700 | 2,694 | 2,694 | 6 {'owner_bound': 6} | 0 | 0 | 0 | 209 | 218 | 39.1 (0.2–39.3) | 92 / 54.1 |
| 10 | 2017f7c2 | focal17* focal18 focal31* | quorum | 2,700 | 2,698 | 2,698 | 2 {'owner_bound': 2} | 0 | 0 | 0 | 212 | 259 | 39.1 (0.1–39.2) | 87 / 49.1 |

totals: started 29,700, written 29,593, judged 29,593, refused 107 {'owner_bound': 40, 'unavailable': 67}, unknown 0, abandoned 0, undelivered 0, resent 1338; judged/started 99.640%
writes (measured window): committed 292,793, refused 107, unknown 0; committed p50/p99/p99.9/max 113.9/16012.9/37755.2/41508.6 ms; schedule delay p99 15.4 ms; wall 138.1 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 35], ['access: Unavailable', 67], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #,#" }', 5]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=17, refused/unavailable=3
  [   5,  10) refused/owner_bound=11, refused/unavailable=51
  [  10,  15) refused/owner_bound=10, refused/unavailable=13
  [  35,  40) refused/owner_bound=2

not committed: 107; started 0.1 to 39.9 s; latency 0.0 to 30.1 s; steps {9: 40, 10: 4, 4: 61, 1: 2}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
