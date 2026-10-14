use super::*;
use std::collections::BTreeSet;

/// A seeded generator, so every failure replays.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn budget() -> MemoryBudget {
    MemoryBudget::new(1 << 30, 1 << 20).unwrap()
}

/// A native index key's shape: a bucket of 16 bytes and a family of two shared by many keys,
/// then a 16-byte identity.
fn index_key(rng: &mut Rng, buckets: u64) -> Vec<u8> {
    let bucket = rng.below(buckets);
    let mut key = vec![0xb0, 0x24, 0x00];
    key.extend_from_slice(&bucket.to_be_bytes());
    key.resize(16, 0);
    key.extend_from_slice(&[0x00, 0x24]);
    key.extend_from_slice(&rng.next().to_be_bytes());
    key.extend_from_slice(&rng.next().to_be_bytes());
    key
}

/// The worst shape for the encoding: keys of any length that share little, or a lot.
fn hostile_key(rng: &mut Rng) -> Vec<u8> {
    let len = 1 + rng.below(MAX_KEY as u64) as usize;
    let shared = rng.below(len as u64) as usize;
    let mut key = vec![0x5a; shared];
    while key.len() < len {
        key.push(rng.next() as u8);
    }
    key
}

fn funding(set: &KeySet, budget: &MemoryBudget, edits: usize) -> KeySetFunding {
    set.fund(budget, BudgetLane::Ordinary, edits).unwrap()
}

fn publish(set: &mut KeySet, budget: &MemoryBudget, edits: &[KeyEdit<'_>]) {
    let mut funding = funding(set, budget, edits.len());
    set.publish(edits, &mut funding).unwrap();
}

fn keys_of(iter: KeyIter<'_>) -> Vec<Vec<u8>> {
    iter.map(|key| key.as_bytes().to_vec()).collect()
}

fn check(set: &KeySet, model: &BTreeSet<Vec<u8>>, rng: &mut Rng) {
    assert_eq!(set.len(), model.len());
    assert_eq!(
        keys_of(set.iter()),
        model.iter().cloned().collect::<Vec<_>>()
    );
    for _ in 0..32 {
        let probe = if model.is_empty() || rng.below(3) == 0 {
            index_key(rng, 4)
        } else {
            let index = rng.below(model.len() as u64) as usize;
            model.iter().nth(index).unwrap().clone()
        };
        assert_eq!(set.contains(&probe), model.contains(&probe));
        for exclusive in [false, true] {
            let expected: Vec<Vec<u8>> = model
                .iter()
                .filter(|key| {
                    if exclusive {
                        key.as_slice() > probe.as_slice()
                    } else {
                        key.as_slice() >= probe.as_slice()
                    }
                })
                .take(40)
                .cloned()
                .collect();
            let found: Vec<Vec<u8>> = set
                .iter_from(&probe, exclusive)
                .take(40)
                .map(|key| key.as_bytes().to_vec())
                .collect();
            assert_eq!(found, expected, "from {probe:02x?} exclusive {exclusive}");
        }
    }
    assert_eq!(set.stats().keys, model.len());
    let root = &set.root;
    assert_eq!(root.len, root.mids.iter().map(|mid| mid.len).sum::<usize>());
    assert!(root.room >= root.mids.len());
    for (fence_value, mid) in root.fences.iter().zip(&root.mids) {
        assert_eq!(*fence_value, fence(mid.first()));
        assert!(!mid.children.is_empty() && mid.children.len() <= MID_CHUNKS);
        assert_eq!(
            mid.len,
            mid.children.iter().map(|chunk| chunk.len).sum::<usize>()
        );
        for (fence_value, chunk) in mid.fences.iter().zip(&mid.children) {
            assert_eq!(*fence_value, fence(chunk.first()));
            assert!(!chunk.children.is_empty() && chunk.children.len() <= CHUNK_BLOCKS);
            assert_eq!(
                chunk.len,
                chunk
                    .children
                    .iter()
                    .map(|block| block.count())
                    .sum::<usize>()
            );
            for (fence_value, block) in chunk.fences.iter().zip(&chunk.children) {
                assert_eq!(*fence_value, fence(block.first()));
                assert!(block.count() <= BLOCK_KEYS);
                assert!(block.entries().len() <= BLOCK_ENTRIES_MAX);
            }
        }
    }
}

/// Random publications of puts and deletes, under frozen images taken between them, against a
/// model; each image reads what the set held when it was taken, and every charge goes once the
/// set and its images do.
fn run(seed: u64, rounds: usize, hostile: bool) {
    let budget = budget();
    let mut rng = Rng(seed);
    {
        let mut set = KeySet::new(&budget, BudgetLane::Ordinary).unwrap();
        let mut model: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut images: Vec<(FrozenKeySet, Vec<Vec<u8>>)> = Vec::new();
        for round in 0..rounds {
            let count = 1 + rng.below(if round % 7 == 0 { 300 } else { 12 }) as usize;
            let mut edits: BTreeSet<Vec<u8>> = BTreeSet::new();
            let mut deletes: BTreeSet<Vec<u8>> = BTreeSet::new();
            for _ in 0..count {
                if !model.is_empty() && rng.below(3) == 0 {
                    let index = rng.below(model.len() as u64) as usize;
                    let key = model.iter().nth(index).unwrap().clone();
                    if !edits.contains(&key) {
                        deletes.insert(key.clone());
                        edits.insert(key);
                    }
                } else {
                    let key = if hostile {
                        hostile_key(&mut rng)
                    } else {
                        index_key(&mut rng, 3)
                    };
                    if !deletes.contains(&key) {
                        edits.insert(key);
                    }
                }
            }
            let batch: Vec<KeyEdit<'_>> = edits
                .iter()
                .map(|key| {
                    if deletes.contains(key) {
                        KeyEdit::Delete(key)
                    } else {
                        KeyEdit::Put(key)
                    }
                })
                .collect();
            publish(&mut set, &budget, &batch);
            for key in &edits {
                if deletes.contains(key) {
                    model.remove(key);
                } else {
                    model.insert(key.clone());
                }
            }
            check(&set, &model, &mut rng);
            if rng.below(5) == 0 {
                images.push((
                    set.freeze(BudgetLane::Committed).unwrap(),
                    model.iter().cloned().collect(),
                ));
            }
            if images.len() > 3 {
                images.remove(0);
            }
            for (image, held) in &images {
                assert_eq!(&keys_of(image.iter()), held);
                assert_eq!(image.len(), held.len());
            }
        }
    }
    assert_eq!(budget.stats().used, 0, "every charge goes with the set");
}

#[test]
fn publications_read_back_as_the_model_does_and_images_keep_what_they_read() {
    for seed in 1..=6 {
        run(seed, 160, false);
    }
}

#[test]
fn hostile_keys_publish_within_their_quote() {
    for seed in 100..=105 {
        run(seed, 120, true);
    }
}

#[test]
fn a_built_set_reads_back_whole_and_splits_and_joins_at_any_key() {
    let budget = budget();
    let mut rng = Rng(7);
    let mut model = BTreeSet::new();
    while model.len() < 90_000 {
        model.insert(index_key(&mut rng, 5));
    }
    {
        let mut builder = KeySetBuilder::new(&budget, BudgetLane::Committed).unwrap();
        for key in &model {
            builder.push(key).unwrap();
        }
        let set = builder.finish().unwrap();
        assert!(set.stats().mids > 1, "the build spans several mids");
        check(&set, &model, &mut rng);
        for _ in 0..12 {
            let at = index_key(&mut rng, 6);
            let (left, right) = set.split_at(&at, BudgetLane::Committed).unwrap();
            let below: BTreeSet<Vec<u8>> = model
                .iter()
                .filter(|key| key.as_slice() < at.as_slice())
                .cloned()
                .collect();
            let above: BTreeSet<Vec<u8>> = model
                .iter()
                .filter(|key| key.as_slice() >= at.as_slice())
                .cloned()
                .collect();
            check(&left, &below, &mut rng);
            check(&right, &above, &mut rng);
            let joined = left.join(&right, BudgetLane::Committed).unwrap();
            check(&joined, &model, &mut rng);
            if !left.is_empty() && !right.is_empty() {
                assert!(right.join(&left, BudgetLane::Committed).is_err());
            }
        }
    }
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn a_refused_edit_leaves_the_set_unchanged_and_unpoisoned() {
    let budget = budget();
    let mut set = KeySet::new(&budget, BudgetLane::Ordinary).unwrap();
    publish(&mut set, &budget, &[KeyEdit::Put(b"b"), KeyEdit::Put(b"d")]);
    let mut funding = funding(&set, &budget, 2);
    assert_eq!(
        set.publish(&[KeyEdit::Put(b"a"), KeyEdit::Delete(b"c")], &mut funding),
        Err(MemoryError::MissingKey)
    );
    assert_eq!(
        set.publish(&[KeyEdit::Put(b"e"), KeyEdit::Put(b"e")], &mut funding),
        Err(MemoryError::DuplicateKey)
    );
    assert!(
        set.publish(&[KeyEdit::Put(&[0u8; MAX_KEY + 1])], &mut funding)
            .is_err()
    );
    assert!(!set.is_poisoned());
    assert_eq!(keys_of(set.iter()), vec![b"b".to_vec(), b"d".to_vec()]);
    publish(
        &mut set,
        &budget,
        &[KeyEdit::Delete(b"b"), KeyEdit::Delete(b"d")],
    );
    assert!(set.is_empty());
    assert_eq!(set.iter().next(), None);
    publish(&mut set, &budget, &[KeyEdit::Put(b"z")]);
    assert_eq!(keys_of(set.iter()), vec![b"z".to_vec()]);
}

#[test]
fn a_publication_short_of_its_funding_poisons_the_set() {
    let budget = budget();
    let mut set = KeySet::new(&budget, BudgetLane::Ordinary).unwrap();
    let mut short = KeySetFunding {
        funding: budget
            .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 8)
            .unwrap()
            .commit(),
        growth: budget
            .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 8)
            .unwrap()
            .commit(),
    };
    assert!(set.publish(&[KeyEdit::Put(b"a")], &mut short).is_err());
    assert!(set.is_poisoned());
    let mut funding = funding(&set, &budget, 1);
    assert!(set.publish(&[KeyEdit::Put(b"b")], &mut funding).is_err());
}

/// A publication spends within its funding whatever frozen images share: under an image taken
/// before every round, each publication copies every chunk and mid it touches.
#[test]
fn a_publication_under_images_spends_within_its_funding() {
    let budget = budget();
    let mut rng = Rng(23);
    let mut set = KeySet::new(&budget, BudgetLane::Ordinary).unwrap();
    let mut model: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut held = Vec::new();
    for _ in 0..400 {
        let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
        for _ in 0..1 + rng.below(16) {
            keys.insert(if rng.below(4) == 0 {
                hostile_key(&mut rng)
            } else {
                index_key(&mut rng, 2)
            });
        }
        let edits: Vec<KeyEdit<'_>> = keys.iter().map(|key| KeyEdit::Put(key)).collect();
        held.push(set.freeze(BudgetLane::Committed).unwrap());
        if held.len() > 2 {
            held.remove(0);
        }
        let mut funding = funding(&set, &budget, edits.len());
        set.publish(&edits, &mut funding).unwrap();
        model.extend(keys);
    }
    check(&set, &model, &mut rng);
}

/// What a native index key costs here, against the row store's 96-byte entry: keys of one
/// bucket shape (16-byte affinity, 2-byte family, 16-byte identity). A measurement printed, and
/// a bound asserted.
#[test]
fn an_index_key_costs_a_fraction_of_a_row_entry() {
    let budget = budget();
    let mut rng = Rng(11);
    let mut model = BTreeSet::new();
    while model.len() < 100_000 {
        model.insert(index_key(&mut rng, 8));
    }
    let mut builder = KeySetBuilder::new(&budget, BudgetLane::Committed).unwrap();
    for key in &model {
        builder.push(key).unwrap();
    }
    let built = builder.finish().unwrap();
    let built_bytes = budget.stats().used;
    // The same keys published in random batches, as a session writes them.
    let mut set = KeySet::new(&budget, BudgetLane::Ordinary).unwrap();
    let mut keys: Vec<&Vec<u8>> = model.iter().collect();
    for index in (1..keys.len()).rev() {
        keys.swap(index, rng.below(index as u64 + 1) as usize);
    }
    for batch in keys.chunks(9) {
        let mut sorted: Vec<&Vec<u8>> = batch.to_vec();
        sorted.sort();
        let edits: Vec<KeyEdit<'_>> = sorted.iter().map(|key| KeyEdit::Put(key)).collect();
        publish(&mut set, &budget, &edits);
    }
    let published_bytes = budget.stats().used - built_bytes;
    let per_built = built_bytes as f64 / model.len() as f64;
    let per_published = published_bytes as f64 / model.len() as f64;
    let stats = set.stats();
    println!(
        "keys {} of 34 bytes: built {per_built:.1} B a key, published {per_published:.1} B a key, {} blocks, {} chunks, {} mids; a publication of 9 funds {} B",
        model.len(),
        stats.blocks,
        stats.chunks,
        stats.mids,
        KeySet::quote(9).unwrap() + KeySet::growth(9).unwrap()
    );
    assert!(per_built < 30.0, "built {per_built}");
    assert!(per_published < 30.0, "published {per_published}");
    drop(built);
}

/// A block rebuilt with its edits by splicing (its keys before the first edit copied as encoded,
/// the rest encoded again) is byte for byte the block encoding all its keys makes: for blocks of
/// every size and shape, index-shaped and hostile, under edits of every kind (puts of new keys and
/// of keys it holds, deletions) and position (before, inside and after its keys).
#[test]
fn a_spliced_block_is_the_block_encoding_every_key_makes() {
    let budget = budget();
    let mut rng = Rng(0x5eed_b10c);
    let mut spliced_any = 0usize;
    for round in 0..4_000 {
        let hostile = round % 3 == 0;
        let mut keys = BTreeSet::new();
        let size = 1 + rng.below(BLOCK_KEYS as u64) as usize;
        while keys.len() < size {
            let key = if hostile {
                hostile_key(&mut rng)
            } else {
                index_key(&mut rng, 4)
            };
            keys.insert(key);
        }
        // One block of the keys, as many as one block takes.
        let mut encoder = Encoder::new().unwrap();
        let mut source = Source::Budget(&budget, BudgetLane::Ordinary);
        let mut taken = Vec::new();
        for key in &keys {
            if encoder.full() {
                break;
            }
            encoder.push(key).unwrap();
            taken.push(key.clone());
        }
        let (_, block) = encoder.finish(&mut source).unwrap();
        // Edits: new keys, held keys put again, held keys deleted, each kept once and sorted.
        let mut edits: std::collections::BTreeMap<Vec<u8>, bool> = Default::default();
        for _ in 0..(1 + rng.below(4)) {
            match rng.below(3) {
                0 => {
                    let key = if hostile {
                        hostile_key(&mut rng)
                    } else {
                        index_key(&mut rng, 4)
                    };
                    if !taken.contains(&key) {
                        edits.insert(key, false);
                    }
                }
                1 => {
                    let key = taken[rng.below(taken.len() as u64) as usize].clone();
                    edits.insert(key, false);
                }
                _ => {
                    let key = taken[rng.below(taken.len() as u64) as usize].clone();
                    edits.insert(key, true);
                }
            }
        }
        if edits.is_empty() {
            continue;
        }
        let edits: Vec<KeyEdit<'_>> = edits
            .iter()
            .map(|(key, delete)| {
                if *delete {
                    KeyEdit::Delete(key)
                } else {
                    KeyEdit::Put(key)
                }
            })
            .collect();
        // Rebuilt as a publication rebuilds it.
        let mut out = Vec::new();
        let mut encoder = Encoder::new().unwrap();
        rebuild_block(&block, &edits, &mut encoder, &mut source, &mut out).unwrap();
        // Every key of the output encoded afresh, cut as the rebuild cuts it.
        let mut count = 0usize;
        let mut counting = Merged::new(&block, &edits);
        while counting.next_key().unwrap().is_some() {
            count += 1;
        }
        let bytes = scaled(block.entries().len(), block.count(), count);
        let per = per_block(count, bytes);
        if per >= count {
            spliced_any += 1;
        }
        let mut whole = Vec::new();
        let mut encoder = Encoder::new().unwrap();
        let mut merged = Merged::new(&block, &edits);
        encode_cut(&mut merged, per, &mut encoder, &mut source, &mut whole).unwrap();
        assert_eq!(out.len(), whole.len(), "round {round}: blocks out");
        for ((first, block), (whole_first, whole_block)) in out.iter().zip(&whole) {
            assert_eq!(first, whole_first, "round {round}: fences");
            assert_eq!(block.bytes, whole_block.bytes, "round {round}: bytes");
        }
    }
    assert!(spliced_any > 1_000, "the splice ran {spliced_any} times");
}

/// What one published key costs a large set fed as the native engine feeds an index family:
/// sixteen streams of ascending identities under a shared prefix (each caller's claims), one new
/// key a stream each round, the set frozen between rounds as a read pins it. Release, with
/// `--ignored --nocapture`.
#[test]
#[ignore = "measurement: run with --ignored --nocapture"]
fn measure_publication_per_key_on_interleaved_streams() {
    const STREAMS: u64 = 16;
    const HELD: u64 = 200_000;
    const ROUNDS: u64 = 4_000;
    let key = |stream: u64, sequence: u64| {
        let mut key = vec![0xb0, 0x24, 0x00, 0x00, 0x24];
        key.resize(18, 0);
        key.extend_from_slice(&stream.to_be_bytes());
        key.extend_from_slice(&sequence.to_be_bytes());
        key
    };
    let budget = MemoryBudget::new(1 << 32, 1 << 20).unwrap();
    let mut set = KeySet::new(&budget, BudgetLane::Ordinary).unwrap();
    let per_stream = HELD / STREAMS;
    let mut held: Vec<Vec<u8>> = (0..STREAMS)
        .flat_map(|stream| (0..per_stream).map(move |sequence| (stream, sequence)))
        .map(|(stream, sequence)| key(stream, sequence))
        .collect();
    held.sort();
    let puts: Vec<KeyEdit<'_>> = held.iter().map(|key| KeyEdit::Put(key)).collect();
    let mut funding = set.fund(&budget, BudgetLane::Ordinary, puts.len()).unwrap();
    set.publish(&puts, &mut funding).unwrap();
    drop(funding);
    let rounds: Vec<Vec<Vec<u8>>> = (0..ROUNDS)
        .map(|round| {
            (0..STREAMS)
                .map(|stream| key(stream, per_stream + round))
                .collect()
        })
        .collect();
    let started = std::time::Instant::now();
    for round in &rounds {
        let frozen = set.freeze(BudgetLane::Ordinary).unwrap();
        let edits: Vec<KeyEdit<'_>> = round.iter().map(|key| KeyEdit::Put(key)).collect();
        let mut funding = set
            .fund(&budget, BudgetLane::Ordinary, edits.len())
            .unwrap();
        set.publish(&edits, &mut funding).unwrap();
        drop(frozen);
    }
    let elapsed = started.elapsed();
    let keys = ROUNDS * STREAMS;
    let started = std::time::Instant::now();
    let mut found = 0u64;
    for round in &rounds {
        for key in round {
            found += u64::from(set.contains(key));
        }
    }
    let contains = started.elapsed();
    assert_eq!(found, keys);
    println!(
        "{} keys held, {keys} published in rounds of {STREAMS}: {:.0} ns a published key; contains {:.0} ns a key",
        set.len(),
        elapsed.as_nanos() as f64 / keys as f64,
        contains.as_nanos() as f64 / keys as f64,
    );
}
