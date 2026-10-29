outcomes in the CSV: {'committed': 146300}

| step | write | writes | p50 ms | p99 ms |
|---|---|---|---|---|
| 1 | `claim.submit` | 13,200 | 35.9 | 90.5 |
| 2 | `claim.post` | 13,217 | 34.4 | 74.4 |
| 3 | `receipt.acquire` | 13,235 | 34.8 | 72.5 |
| 4 | `artifact.submit` | 13,252 | 37.5 | 78.2 |
| 5 | `testament.submit` | 13,274 | 35.0 | 74.4 |
| 6 | `testament.post` | 13,290 | 34.8 | 73.4 |
| 7 | `testament.receive` | 13,314 | 34.9 | 72.6 |
| 8 | `validation.enter_whole_work` | 13,331 | 34.5 | 72.2 |
| 9 | `validation.begin` | 13,364 | 35.8 | 73.9 |
| 10 | `validation.report` | 13,400 | 37.2 | 81.1 |
| 11 | `claim.release_scope` | 13,423 | 35.0 | 73.6 |

| ledger | session | writes | p50 ms | p99 ms |
|---|---|---|---|---|
| 0 | 2d4e14ce | 13,300 | 36.0 | 72.3 |
| 1 | 26a9ba97 | 13,308 | 35.5 | 91.2 |
| 2 | 11b4ef0f | 13,294 | 35.4 | 71.5 |
| 3 | d893d8d0 | 13,294 | 35.0 | 73.6 |
| 4 | 154558a6 | 13,294 | 35.3 | 76.3 |
| 5 | a46e93a6 | 13,300 | 35.7 | 79.3 |
| 6 | 58a3c275 | 13,304 | 35.9 | 79.8 |
| 7 | a0465a72 | 13,299 | 35.4 | 86.5 |
| 8 | a5b21279 | 13,305 | 35.2 | 69.5 |
| 9 | 9a72ce98 | 13,305 | 35.5 | 76.2 |
| 10 | a9692f9e | 13,297 | 35.0 | 74.3 |

over the load (84.1 s):

| role | nodes | CPU mean | CPU max |
|---|---|---|---|
| generator | 1 | 0.716 | 0.716 |
| founder (root, directory, its session's leader) | 1 | 0.534 | 0.534 |
| other sessions' leaders | 10 | 0.373 | 0.377 |
| followers | 22 | 0.314 | 0.351 |
| all 33 nodes | 33 | total 11.16 CPUs | |
throttled periods: generator 0 {'focal1': '5/840', 'focal10': '2/841', 'focal11': '1/839', 'focal12': '1/839', 'focal13': '1/840', 'focal14': '2/841', 'focal15': '1/840', 'focal16': '3/841', 'focal17': '2/840', 'focal18': '2/840', 'focal19': '1/840', 'focal2': '2/840', 'focal20': '3/838', 'focal21': '1/839', 'focal22': '2/841', 'focal23': '2/841', 'focal24': '2/841', 'focal25': '2/840', 'focal26': '1/840', 'focal27': '1/840', 'focal28': '2/841', 'focal29': '1/840', 'focal3': '3/840', 'focal30': '1/840', 'focal31': '2/841', 'focal32': '2/841', 'focal33': '2/840', 'focal4': '1/840', 'focal5': '2/840', 'focal6': '1/839', 'focal7': '2/839', 'focal8': '3/841', 'focal9': '2/839'}
disk nvme1n1: 18,492 writes/s, 276 flushes/s, 242.0 MB/s
load: the 33 nodes wrote 149.7 MB/s in 12,841 writes/s over 84.1 s; most s5-focal3-1 5.4 MB/s
  per committed write: 69.3 KB (181,500 writes)
rest: the 33 nodes wrote 1.0 MB/s in 99 writes/s over 62.5 s; most s5-focal1-1 0.2 MB/s
