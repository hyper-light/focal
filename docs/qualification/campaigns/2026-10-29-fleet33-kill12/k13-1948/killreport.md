## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.7 s, started again at 37.7 s; serving 38.4 to 40.0 s (12 of 12), 0.7 to 2.3 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0374e155 | focal1* focal17* focal24 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 149 | 225 | 37.0 (0.2–37.2) | 64 / 26.3 |
| 1 | ce7de2d6 | focal4 focal18 focal23* | a follower | 2,700 | 2,651 | 2,651 | 49 {'unavailable': 49} | 0 | 0 | 0 | 23 | 188 | 14.7 (15.6–30.3) | 82 / 44.3 |
| 2 | 5fdb8b6c | focal7* focal9* focal11 | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 145 | 195 | 36.6 (0.2–36.7) | 58 / 20.3 |
| 3 | 320f482d | focal8* focal10 focal15 | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 197 | 3.2 (32.4–35.6) | 66 / 28.3 |
| 4 | 02c87eb4 | focal13 focal30 focal32* | a follower | 2,700 | 2,648 | 2,648 | 52 {'unavailable': 52} | 0 | 0 | 0 | 25 | 226 | 14.7 (15.6–30.3) | 56 / 18.3 |
| 5 | 34cd16ee | focal6* focal14 focal16* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 180 | 189 | 38.6 (0.1–38.7) | 65 / 27.3 |
| 6 | a065d1f0 | focal19 focal26 focal33 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 205 | 3.1 (32.4–35.5) | 58 / 20.3 |
| 7 | 54d29fff | focal22 focal21 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 189 | 1.1 (33.1–34.2) | 58 / 20.3 |
| 8 | 1a0bdbeb | focal3 focal20 focal25 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 191 | 2.6 (32.4–35.0) | 58 / 20.3 |
| 9 | a1078f92 | focal5 focal27* focal28* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 146 | 203 | 37.0 (0.2–37.1) | 66 / 28.3 |
| 10 | 8149ab77 | focal2 focal12 focal31* | leader | 2,700 | 2,691 | 2,691 | 9 {'owner_bound': 9} | 0 | 0 | 0 | 35 | 185 | 20.0 (20.8–40.8) | 90 / 52.3 |

totals: started 29,700, written 29,589, judged 29,589, refused 111 {'owner_bound': 10, 'unavailable': 101}, unknown 0, abandoned 0, undelivered 0, resent 703; judged/started 99.626%
writes (measured window): committed 290,610, refused 111, unknown 0; committed p50/p99/p99.9/max 59.2/21951.8/34294.4/41722.7 ms; schedule delay p99 12173.1 ms; wall 136.0 s
refusals (reasons): [['access: Unavailable', 101], ['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 10]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=1, refused/unavailable=34
  [   5,  10) refused/unavailable=66
  [  10,  15) refused/owner_bound=9, refused/unavailable=1

not committed: 111; started 0.2 to 12.5 s; latency 7.7 to 30.1 s; steps {9: 10, 4: 96, 10: 5}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 0
sessions stopped on a node: 1
  focal8 320f482d: native: native lifecycle admission: a configured bound or counter would be exceeded (diag capacity sites ["crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_index.rs:55 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_index.rs:58 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_index.rs:55 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_index.rs:58 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_index.rs:55 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"main\")", "crates/focal-core/src/native/record_codec/read_claim_build.rs:152 on Some(\"focal-node-sessions-14980483432396755493-0\")"])
