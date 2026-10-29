outcomes in the CSV: {'committed': 146410}

| step | write | writes | p50 ms | p99 ms |
|---|---|---|---|---|
| 1 | `claim.submit` | 13,200 | 35.1 | 174.0 |
| 2 | `claim.post` | 13,220 | 34.1 | 142.4 |
| 3 | `receipt.acquire` | 13,244 | 34.3 | 145.0 |
| 4 | `artifact.submit` | 13,263 | 36.4 | 146.6 |
| 5 | `testament.submit` | 13,287 | 34.1 | 145.9 |
| 6 | `testament.post` | 13,306 | 34.2 | 146.5 |
| 7 | `testament.receive` | 13,327 | 34.1 | 146.5 |
| 8 | `validation.enter_whole_work` | 13,344 | 33.8 | 145.2 |
| 9 | `validation.begin` | 13,375 | 35.0 | 145.3 |
| 10 | `validation.report` | 13,410 | 36.0 | 144.4 |
| 11 | `claim.release_scope` | 13,434 | 33.8 | 143.4 |

| ledger | session | writes | p50 ms | p99 ms |
|---|---|---|---|---|
| 0 | 4a8fb3fc | 13,296 | 35.4 | 147.9 |
| 1 | 4b3d81f4 | 13,305 | 34.6 | 147.6 |
| 2 | 823fb39c | 13,307 | 34.6 | 149.9 |
| 3 | f89cc0db | 13,312 | 35.0 | 147.1 |
| 4 | c9afc150 | 13,306 | 34.4 | 147.0 |
| 5 | d0b4e782 | 13,312 | 34.2 | 146.4 |
| 6 | 535cdc0d | 13,312 | 34.5 | 146.2 |
| 7 | c2667a1b | 13,305 | 34.5 | 146.7 |
| 8 | 96a53220 | 13,324 | 34.4 | 147.7 |
| 9 | 1df9cc49 | 13,312 | 34.4 | 145.6 |
| 10 | 23e5faba | 13,319 | 35.1 | 150.4 |

over the load (81.6 s):

| role | nodes | CPU mean | CPU max |
|---|---|---|---|
| generator | 1 | 0.644 | 0.644 |
| founder (root, directory, its session's leader) | 1 | 0.443 | 0.443 |
| other sessions' leaders | 10 | 0.312 | 0.314 |
| followers | 22 | 0.253 | 0.281 |
| all 33 nodes | 33 | total 9.13 CPUs | |
throttled periods: generator 0 {'focal1': '3/816', 'focal10': '4/815', 'focal12': '1/816', 'focal14': '1/816', 'focal16': '2/815', 'focal19': '1/816', 'focal2': '1/816', 'focal22': '1/816', 'focal25': '1/816', 'focal26': '2/816', 'focal27': '1/817', 'focal28': '1/817', 'focal29': '1/817', 'focal3': '1/817', 'focal30': '4/815', 'focal31': '1/816', 'focal32': '1/815', 'focal33': '1/816', 'focal6': '1/816', 'focal8': '1/817', 'focal9': '1/814'}
disk nvme1n1: 19,474 writes/s, 336 flushes/s, 257.7 MB/s
