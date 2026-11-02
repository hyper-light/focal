## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 2.9 s, started again at 39.3 s; serving 40.0 to 42.9 s (12 of 12), 0.7 to 3.7 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 16283f27 | focal1* focal5 focal6* | quorum | 2,700 | 2,697 | 2,697 | 3 {'unavailable': 3} | 0 | 0 | 0 | 267 | 337 | 39.8 (0.2–40.0) | 83 / 43.7 |
| 1 | 4115f408 | focal4 focal17* focal21 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 210 | 0.4 (1.4–1.8) | 97 / 57.7 |
| 2 | a647f88e | focal7* focal24 focal32* | quorum | 2,700 | 2,696 | 2,696 | 4 {'owner_bound': 4} | 0 | 0 | 0 | 201 | 276 | 38.3 (0.2–38.6) | 86 / 46.7 |
| 3 | 6290f951 | focal10 focal26 focal27* | a follower | 2,700 | 2,677 | 2,677 | 23 {'unavailable': 23} | 0 | 0 | 0 | 241 | 275 | 0.5 (0.5–1.0) | 88 / 48.7 |
| 4 | 10107fe3 | focal3 focal11 focal13 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 235 | 0.4 (0.7–1.1) | 91 / 51.7 |
| 5 | 4f634ebf | focal16* focal29 focal30 | leader | 2,700 | 2,670 | 2,670 | 30 {'owner_bound': 19, 'unavailable': 11} | 0 | 0 | 0 | 141 | 282 | 2.4 (0.2–2.7) | 83 / 43.7 |
| 6 | 2f590172 | focal18 focal19 focal20 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 247 | 0.5 (0.5–1.0) | 98 / 58.7 |
| 7 | 5c1c4fc1 | focal8* focal15 focal22 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 224 | 0.5 (0.6–1.1) | 87 / 47.7 |
| 8 | 4092ec6d | focal12 focal23* focal25 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 215 | 0.4 (80.0–80.5) | 91 / 51.7 |
| 9 | 12ffcb99 | focal9* focal14 focal28* | quorum | 2,700 | 2,695 | 2,695 | 5 {'unavailable': 5} | 0 | 0 | 0 | 278 | 294 | 40.6 (0.3–40.9) | 83 / 43.7 |
| 10 | e8ec4783 | focal2 focal31* focal33 | leader | 2,700 | 2,640 | 2,640 | 60 {'owner_bound': 19, 'unavailable': 41} | 0 | 0 | 0 | 226 | 281 | 1.5 (0.3–1.7) | 83 / 43.7 |

totals: started 29,700, written 29,575, judged 29,575, refused 125 {'owner_bound': 42, 'unavailable': 83}, unknown 0, abandoned 0, undelivered 0, resent 1354; judged/started 99.579%
writes (measured window): committed 290,696, refused 125, unknown 0; committed p50/p99/p99.9/max 180.3/15470.0/38601.2/45916.7 ms; schedule delay p99 54.4 ms; wall 137.7 s
refusals (reasons): [['access: Unavailable', 83], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 42]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=19, refused/unavailable=7
  [   5,  10) refused/owner_bound=8, refused/unavailable=43
  [  10,  15) refused/owner_bound=15, refused/unavailable=33

not committed: 125; started 0.2 to 13.9 s; latency 0.0 to 30.1 s; steps {1: 8, 9: 42, 4: 74, 10: 1}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
