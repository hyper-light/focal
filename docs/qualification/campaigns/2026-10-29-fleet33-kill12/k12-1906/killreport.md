## focal-220-128-work-agents500-nodes33-faults.json

killed (12): focal1 focal6 focal7 focal8 focal9 focal16 focal17 focal23 focal27 focal28 focal31 focal32; all killed by 1.3 s, started again at 34.5 s; serving 34.9 to 36.6 s (12 of 12), 0.4 to 2.1 s after their start

| ledger | session | voters (killed *) | lost | started | written | judged | refused (by cause) | unknown | abandoned | undelivered | resent | slowest before (ms) | longest without a commit (s: from–to after the kill) | recovered at (s from kill / from start) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | fc43f9a1 | focal1* focal3 focal14 | leader | 2,700 | 2,691 | 2,691 | 9 {'owner_bound': 9} | 0 | 0 | 0 | 2 | 2080 | 15.6 (20.5–36.2) | 51 / 16.5 |
| 1 | d4bdaaf2 | focal4 focal5 focal9* | a follower | 2,700 | 2,687 | 2,687 | 0  | 0 | 0 | 13 | 0 | 2201 | 60.3 (219.9–280.2) | 94 / 59.5 |
| 2 | f9325afb | focal8* focal7* focal33 | quorum | 2,700 | 2,578 | 2,578 | 122 {'unavailable': 122} | 0 | 0 | 0 | 29 | 2408 | 36.7 (0.2–36.8) | 48 / 13.5 |
| 3 | 4f022a38 | focal10 focal18 focal20 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 2780 | 2.8 (59.8–62.7) | 61 / 26.5 |
| 4 | 142af667 | focal13 focal21 focal32* | a follower | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 1 | 3298 | 19.9 (15.3–35.2) | 42 / 7.5 |
| 5 | 60808160 | focal6* focal11 focal16* | quorum | 2,700 | 2,699 | 2,699 | 1 {'owner_bound': 1} | 0 | 0 | 0 | 103 | 2449 | 34.6 (0.2–34.8) | 61 / 26.5 |
| 6 | b9940f2e | focal12 focal19 focal26 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 2199 | 1.6 (59.8–61.4) | 0 / -34.5 |
| 7 | 049a02f6 | focal22 focal23* focal30 | a follower | 2,700 | 2,647 | 2,647 | 53 {'unavailable': 53} | 0 | 0 | 0 | 12 | 2124 | 14.6 (15.6–30.2) | 61 / 26.5 |
| 8 | 696b0f4d | focal15 focal25 focal29 | nothing | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 0 | 2108 | 3.1 (59.8–62.9) | 61 / 26.5 |
| 9 | 630295be | focal17* focal28* focal27* | quorum | 2,700 | 2,700 | 2,700 | 0  | 0 | 0 | 0 | 244 | 2126 | 37.0 (0.2–37.2) | 61 / 26.5 |
| 10 | e52a209e | focal2 focal24 focal31* | leader | 2,700 | 2,693 | 2,693 | 7 {'owner_bound': 7} | 0 | 0 | 0 | 2 | 2072 | 13.7 (20.5–34.2) | 94 / 59.5 |

totals: started 29,700, written 29,495, judged 29,495, refused 192 {'owner_bound': 17, 'unavailable': 175}, unknown 0, abandoned 0, undelivered 13, resent 393; judged/started 99.310%
writes (measured window): committed 300,305, refused 192, unknown 0; committed p50/p99/p99.9/max 109.1/20975.2/45198.2/67592.3 ms; schedule delay p99 12629.7 ms; wall 380.0 s
refusals (reasons): [['refused: NativeRefusal { kind: Capacity, detail: "custody: # of # required copies hold the artifact; missing nodes #" }', 17], ['access: Unavailable', 175]]

writes not committed, by start (5 s windows from the kill): outcome/cause=count
  [   0,   5) refused/owner_bound=4, refused/unavailable=122
  [   5,  10) refused/unavailable=53
  [  10,  15) refused/owner_bound=13

not committed: 192; started 0.2 to 12.5 s; latency 7.7 to 30.1 s; steps {9: 17, 1: 122, 10: 4, 4: 49}

liveness at the end: nodes holding a member dead: 0 of 33 (most 0)
placement agents retrying or failed at the end: 1
  focal14: None metadata owner: metadata outcome is unknown; retry the same identity
sessions stopped on a node: 1
  focal8 049a02f6: native: native lifecycle admission: a configured bound or counter would be exceeded (diag capacity sites ["crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_index.rs:55 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_index.rs:58 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_index.rs:55 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_index.rs:58 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_index.rs:55 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/replay_validate.rs:48 on Some(\"focal-node-sessions-9495021603782004691-0\")", "crates/focal-core/src/native/record_codec/read_validate.rs:624 on Some(\"focal-node-sessions-9495021603782004691-0\")"])
