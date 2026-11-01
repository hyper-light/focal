## focal-220-128-work-agents500-nodes33-faults.json

isolated (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all cut off by 0.1 s, joined again at 30.3 s (every time from the isolation's start; 'from start' below is from the rejoin)

| ledger | session | voters (cut off *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the isolation) | recovered at (s from the isolation / from the rejoin) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 885b5ce7 | focal1* focal2 focal3 | leader | 2,700 | 2,655 | 2,655 | 45 {'owner_bound': 15, 'unavailable': 30} | 0 | 0 | 0 | 45 | 977 | 1.7 (0.1–1.8) | 62 / 31.7 |
| 1 | e9c7177c | focal4 focal17* focal21 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 16 | 1046 | 1.1 (61.2–62.3) | 62 / 31.7 |
| 2 | bc2dfc98 | focal6* focal7* focal23* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 324 | 1004 | 31.0 (0.1–31.1) | 62 / 31.7 |
| 3 | 218c0f49 | focal9* focal8* focal10 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 144 | 1048 | 31.6 (0.1–31.8) | 62 / 31.7 |
| 4 | 0aa7fbca | focal13 focal27* focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 74 | 979 | 31.9 (0.1–32.1) | 62 / 31.7 |
| 5 | 7064703a | focal14 focal16* focal18 | leader | 2,700 | 2,682 | 2,682 | 18 {'owner_bound': 18} | 0 | 0 | 0 | 52 | 997 | 1.9 (0.1–2.0) | 62 / 31.7 |
| 6 | a660376a | focal11 focal12 focal19 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 964 | 1.1 (61.2–62.3) | 62 / 31.7 |
| 7 | 1e26f18c | focal5 focal22 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1012 | 1.1 (61.2–62.3) | 62 / 31.7 |
| 8 | c9fbaf4c | focal20 focal25 focal30 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 1003 | 1.1 (61.2–62.3) | 62 / 31.7 |
| 9 | be4e5d47 | focal15 focal26 focal28* | leader | 2,700 | 2,680 | 2,680 | 20 {'owner_bound': 20} | 0 | 0 | 0 | 17 | 990 | 1.5 (0.1–1.6) | 62 / 31.7 |
| 10 | abee5238 | focal24 focal29 focal31* | leader | 2,700 | 2,678 | 2,678 | 22 {'owner_bound': 22} | 0 | 0 | 0 | 45 | 931 | 2.4 (0.1–2.5) | 62 / 31.7 |

totals: started 29,700, written 29,594, judged 29,594, refused 106 {'owner_bound': 76, 'unavailable': 30}, unknown 0, abandoned 0, undelivered 0, resent 717; judged/started 99.643%
writes (measured window): committed 291,136, refused 106, unknown 0; committed p50/p99/p99.9/max 105.3/16306.7/31019.2/36988.8 ms; schedule delay p99 14.9 ms; wall 135.8 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 76], ['access: Unavailable', 30]]

writes not committed, by start (5 s windows from the isolation): outcome/cause=count
  [   0,   5) refused/owner_bound=45, refused/unavailable=29
  [   5,  10) refused/owner_bound=8, refused/unavailable=1
  [  10,  15) refused/owner_bound=23

not committed: 106; started 0.0 to 12.4 s; latency 0.0 to 33.0 s; steps {9: 76, 4: 27, 10: 3}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 0
