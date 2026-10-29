## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.1 s, started again at 33.8 s; serving 34.1 to 36.4 s (12 of 12), 0.3 to 2.6 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | d9e4dd36 | focal1* focal6* focal32* | quorum | 2,700 | 2,569 | 2,570 | 0  | 119 | 12 | 0 | - | 104 | 35.4 | 97 / 63.2 |
| 1 | a079bed6 | focal3 focal4 focal5 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | - | 141 | 0.5 | 102 / 68.2 |
| 2 | 31d5961d | focal7* focal29 focal33 | leader | 2,700 | 2,517 | 2,519 | 0  | 167 | 16 | 0 | - | 89 | 37.1 | 102 / 68.2 |
| 3 | 9a9095ab | focal2 focal10 focal9* | a follower | 2,700 | 847 | 847 | 1,519 {'unavailable': 1519} | 334 | 0 | 0 | - | 92 | 7.8 | 184 / 150.2 |
| 4 | 0f89aab4 | focal8* focal12 focal13 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | - | 108 | 4.3 | 102 / 68.2 |
| 5 | a8e0099d | focal15 focal16* focal20 | leader | 2,700 | 1,708 | 1,709 | 449 {'unavailable': 449} | 430 | 37 | 76 | - | 110 | 36.8 | 136 / 102.2 |
| 6 | 6c0a7233 | focal19 focal23* focal30 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | - | 123 | 5.4 | 102 / 68.2 |
| 7 | 6706bd59 | focal11 focal18 focal22 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | - | 101 | 0.5 | 102 / 68.2 |
| 8 | 5956c62f | focal17* focal27* focal25 | quorum | 2,700 | 844 | 844 | 1,528 {'unavailable': 1528} | 315 | 13 | 0 | - | 141 | 36.2 | 192 / 158.2 |
| 9 | f0fddd37 | focal14 focal24 focal28* | leader | 2,700 | 2,508 | 2,511 | 0  | 184 | 8 | 0 | - | 121 | 37.2 | 102 / 68.2 |
| 10 | 7e79aa93 | focal21 focal26 focal31* | leader | 2,700 | 2,525 | 2,527 | 0  | 160 | 15 | 0 | - | 119 | 37.6 | 102 / 68.2 |

totals: started 29,700, written 24,318, judged 24,327, refused 3,496 {'unavailable': 3496}, unknown 1709, abandoned 101, undelivered 76, resent -; judged/started 81.909%
writes (measured window): committed 244,705, refused 3,496, unknown 1,709; committed p50/p99/p99.9/max 43.6/36057.2/85145.4/102156.8 ms; schedule delay p99 35675.2 ms; wall 300.0 s
refusals (reasons): [['lifecycle read: client: transport unavailable', 64], ['client: outcome unknown for request #ecd; retry the same identity', 2], ['client: outcome unknown for request #edf; retry the same identity', 17], ['client: outcome unknown for request #ef#; retry the same identity', 18], ['client: outcome unknown for request #f#b; retry the same identity', 17], ['client: outcome unknown for request #f#; retry the same identity', 70], ['client: outcome unknown for request #f#d; retry the same identity', 10], ['(other reasons)', 5119]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/unavailable=111, unknown/-=526
  [   5,  10) refused/unavailable=61, unknown/-=330
  [  10,  15) refused/unavailable=76, unknown/-=57
  [  15,  20) refused/unavailable=15, unknown/-=20
  [  20,  25) refused/unavailable=26, unknown/-=12
  [  25,  30) refused/unavailable=23, unknown/-=5
  [  30,  35) refused/unavailable=103, unknown/-=11
  [  35,  40) refused/unavailable=354, unknown/-=53
  [  40,  45) refused/unavailable=187, unknown/-=30
  [  45,  50) refused/unavailable=62, unknown/-=13
  [  50,  55) refused/unavailable=55, unknown/-=16
  [  55,  60) refused/unavailable=27, unknown/-=12
  [  60,  65) refused/unavailable=99, unknown/-=18
  [  65,  70) refused/unavailable=192, unknown/-=41
  [  70,  75) refused/unavailable=293, unknown/-=55
  [  75,  80) refused/unavailable=46, unknown/-=16
  [  80,  85) refused/unavailable=56, unknown/-=25
  [  85,  90) refused/unavailable=22, unknown/-=15
  [  90,  95) refused/unavailable=80, unknown/-=20
  [  95, 100) refused/unavailable=138, unknown/-=34
  [ 100, 105) refused/unavailable=301, unknown/-=43
  [ 105, 110) refused/unavailable=25, unknown/-=7
  [ 110, 115) refused/unavailable=27, unknown/-=15
  [ 115, 120) refused/unavailable=3, unknown/-=3
  [ 120, 125) refused/unavailable=71, unknown/-=13
  [ 125, 130) refused/unavailable=129, unknown/-=19
  [ 130, 135) refused/unavailable=317, unknown/-=50
  [ 135, 140) refused/unavailable=28
  [ 140, 145) refused/unavailable=22, unknown/-=8
  [ 150, 155) refused/unavailable=66, unknown/-=11
  [ 155, 160) refused/unavailable=119, unknown/-=19
  [ 160, 165) refused/unavailable=268, unknown/-=97
  [ 165, 170) refused/unavailable=11, unknown/-=19
  [ 170, 175) refused/unavailable=14, unknown/-=16
  [ 180, 185) refused/unavailable=34, unknown/-=3
  [ 185, 190) refused/unavailable=20, unknown/-=37
  [ 190, 195) refused/unavailable=15, unknown/-=40

not committed: 5205; started 0.2 to 192.0 s; latency 22.9 to 74.3 s; steps {10: 20, 9: 77, 5: 6, 8: 3, 3: 114, 1: 1398, 4: 3559, 6: 5, 7: 10, 2: 11, 11: 2}

liveness at the end: nodes holding a member dead: 21 of 33 (most 12)
placement agents retrying or failed at the end: 0
