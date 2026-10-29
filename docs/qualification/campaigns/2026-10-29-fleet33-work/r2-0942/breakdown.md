outcomes in the CSV: {'committed': 147545}

| step | write | writes | p50 ms | p99 ms |
|---|---|---|---|---|
| 1 | `claim.submit` | 13,200 | 42.6 | 118.2 |
| 2 | `claim.post` | 13,232 | 41.4 | 105.5 |
| 3 | `receipt.acquire` | 13,266 | 41.8 | 107.5 |
| 4 | `artifact.submit` | 13,295 | 44.2 | 108.3 |
| 5 | `testament.submit` | 13,331 | 41.9 | 105.2 |
| 6 | `testament.post` | 13,373 | 41.5 | 104.9 |
| 7 | `testament.receive` | 13,415 | 41.4 | 106.9 |
| 8 | `validation.enter_whole_work` | 13,453 | 41.2 | 107.5 |
| 9 | `validation.begin` | 13,542 | 42.6 | 106.9 |
| 10 | `validation.report` | 13,677 | 44.1 | 109.8 |
| 11 | `claim.release_scope` | 13,761 | 41.7 | 107.5 |

| ledger | session | writes | p50 ms | p99 ms |
|---|---|---|---|---|
| 0 | 149117f8 | 13,408 | 41.6 | 99.8 |
| 1 | 2cdaf45d | 13,409 | 41.6 | 107.0 |
| 2 | 3fa32035 | 13,397 | 41.9 | 100.6 |
| 3 | 5b300075 | 13,395 | 42.2 | 110.8 |
| 4 | 406993a4 | 13,409 | 42.9 | 110.9 |
| 5 | e9c1a319 | 13,397 | 42.6 | 106.2 |
| 6 | 2d885323 | 13,418 | 43.0 | 106.1 |
| 7 | ddb0bb31 | 13,402 | 42.2 | 125.8 |
| 8 | 030f50be | 13,438 | 41.9 | 107.1 |
| 9 | c94c1d55 | 13,449 | 42.7 | 123.2 |
| 10 | 9acb1b57 | 13,423 | 41.4 | 102.4 |

over the load (83.3 s):

| role | nodes | CPU mean | CPU max |
|---|---|---|---|
| generator | 1 | 0.692 | 0.692 |
| founder (root, directory, its session's leader) | 1 | 0.468 | 0.468 |
| other sessions' leaders | 10 | 0.321 | 0.326 |
| followers | 22 | 0.261 | 0.293 |
| all 33 nodes | 33 | total 9.43 CPUs | |
throttled periods: generator 0 {'focal1': '3/833', 'focal10': '3/833', 'focal11': '2/834', 'focal12': '1/832', 'focal13': '2/834', 'focal14': '1/831', 'focal15': '1/833', 'focal17': '2/832', 'focal18': '1/834', 'focal19': '1/834', 'focal2': '1/833', 'focal20': '1/834', 'focal21': '1/834', 'focal22': '1/833', 'focal23': '2/833', 'focal24': '1/831', 'focal25': '1/833', 'focal26': '1/831', 'focal27': '1/833', 'focal28': '1/833', 'focal29': '2/830', 'focal3': '2/833', 'focal30': '1/834', 'focal31': '2/834', 'focal32': '2/833', 'focal33': '2/833', 'focal4': '1/833', 'focal5': '1/833', 'focal6': '2/833', 'focal7': '2/833', 'focal8': '1/834', 'focal9': '2/833'}
disk nvme1n1: 18,051 writes/s, 364 flushes/s, 271.5 MB/s
