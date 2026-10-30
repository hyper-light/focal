## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 4.0 s, started again at 37.0 s; serving 37.7 to 39.2 s (12 of 12), 0.7 to 2.2 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 7791b007 | focal1* focal26 focal33 | leader | 2,700 | 2,697 | 2,697 | 3 {'owner_bound': 3} | 0 | 0 | 0 | 2 | 154 | 10.0 (0.2–10.2) | 90 / 53.0 |
| 1 | cd5740d0 | focal4 focal9* focal29 | a follower | 2,700 | 2,698 | 2,698 | 2 {'ingress_capacity': 2} | 0 | 0 | 0 | 0 | 171 | 4.2 (17.6–21.8) | 90 / 53.0 |
| 2 | d95a2aca | focal7* focal21 focal23* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 181 | 153 | 38.5 (0.2–38.7) | 93 / 56.0 |
| 3 | 41287e6d | focal6* focal10 focal14 | a follower | 2,700 | 1,723 | 1,759 | 818 {'unavailable': 818} | 0 | 149 | 10 | 11 | 153 | 6.5 (33.3–39.8) | 93 / 56.0 |
| 4 | 86bdc20f | focal8* focal13 focal12 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 161 | 4.7 (35.1–39.7) | 93 / 56.0 |
| 5 | c4f196d0 | focal16* focal30 focal32* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 158 | 153 | 37.8 (0.2–38.0) | 91 / 54.0 |
| 6 | 0d562917 | focal5 focal19 focal27* | a follower | 2,700 | 1,580 | 1,620 | 915 {'unavailable': 915} | 0 | 184 | 21 | 11 | 149 | 4.7 (33.8–38.5) | 93 / 56.0 |
| 7 | 4335a8ac | focal3 focal2 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 148 | 0.8 (89.2–90.1) | 90 / 53.0 |
| 8 | 35cfbd39 | focal11 focal18 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 141 | 0.8 (89.2–90.1) | 93 / 56.0 |
| 9 | 95e815be | focal15 focal17* focal28* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 163 | 153 | 37.6 (0.2–37.8) | 93 / 56.0 |
| 10 | 732625d3 | focal20 focal24 focal31* | leader | 2,700 | 2,698 | 2,698 | 2 {'owner_bound': 2} | 0 | 0 | 0 | 186 | 157 | 38.5 (0.2–38.7) | 93 / 56.0 |

totals: started 29,700, written 27,596, judged 27,672, refused 1,740 {'owner_bound': 5, 'ingress_capacity': 2, 'unavailable': 1733}, unknown 0, abandoned 333, undelivered 31, resent 712; judged/started 93.172%
writes (measured window): committed 271,667, refused 1,740, unknown 0; committed p50/p99/p99.9/max 64.1/20527.5/36103.9/43413.4 ms; schedule delay p99 20994.9 ms; wall 281.5 s
refusals (reasons): [['access: Capacity', 2], ['access: Unavailable', 1733], ['lifecycle read: client: authoritative ledger service is unavailable', 332], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 5], ['lifecycle read: client: transport unavailable', 1]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=1
  [  10,  15) refused/owner_bound=2
  [  15,  20) refused/ingress_capacity=2
  [  20,  25) refused/owner_bound=2
  [  50,  55) refused/unavailable=60
  [  55,  60) refused/unavailable=377
  [  60,  65) refused/unavailable=200
  [  65,  70) refused/unavailable=200
  [  70,  75) refused/unavailable=200
  [  75,  80) refused/unavailable=200
  [  80,  85) refused/unavailable=200
  [  85,  90) refused/unavailable=200
  [  90,  95) refused/unavailable=96

not committed: 1740; started 0.2 to 92.4 s; latency 1.1 to 163.1 s; steps {9: 18, 1: 1484, 4: 19, 11: 43, 10: 47, 8: 13, 7: 14, 6: 28, 5: 33, 3: 25, 2: 16}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 2
  focal12: None content custody: mutation outcome is unknown; recover the exact saved request
  focal21: None content custody: mutation outcome is unknown; recover the exact saved request
sessions stopped on a node: 0
