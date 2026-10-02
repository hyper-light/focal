//! A path between real nodes, as a relay shapes it: every datagram each
//! way travels a delay, with jitter drawn for each, and where a rate is
//! given waits its turn behind those before it at that rate, one that
//! finds the queue full dropped. A node binds where the
//! relay forwards to (`--listen`) and advertises the relay's front, so what
//! its peers send it, and what it answers, cross the relay. The relay keeps
//! one socket per client it has seen, so the node behind it sees each
//! client as its own address and answers come back to the client that
//! asked; it holds a bounded number of clients and forgets the one longest
//! quiet when a new one comes.
#![allow(dead_code)]
use std::{
    collections::{BinaryHeap, VecDeque},
    net::{SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const CLIENTS: usize = 16;
const DATAGRAM: usize = 65_536;
const POLL: Duration = Duration::from_millis(1);

/// A datagram waiting to travel.
struct Travelling {
    at: Instant,
    serial: u64,
    to: SocketAddr,
    via: usize,
    bytes: Vec<u8>,
}
impl PartialEq for Travelling {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.serial == other.serial
    }
}
impl Eq for Travelling {}
impl PartialOrd for Travelling {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Travelling {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // The soonest first: a max-heap of the reverse.
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.serial.cmp(&self.serial))
    }
}

pub struct Relay {
    front: SocketAddr,
    stop: Arc<AtomicBool>,
    /// The bytes carried toward the node behind, and toward its clients.
    carried: Arc<[AtomicU64; 4]>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Relay {
    /// A relay in front of `back`, each datagram each way delayed by `delay`
    /// and up to `jitter` more: a round trip of twice the delay.
    pub fn new(back: SocketAddr, delay: Duration, jitter: Duration) -> Self {
        Self::shaped(back, delay, jitter, None)
    }
    /// The same, carrying `bits` a second each way where given.
    pub fn shaped(back: SocketAddr, delay: Duration, jitter: Duration, bits: Option<u64>) -> Self {
        let front_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let front = front_socket.local_addr().unwrap();
        front_socket.set_read_timeout(Some(POLL)).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let carried: Arc<[AtomicU64; 4]> = Arc::new([
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
        ]);
        let counting = carried.clone();
        let thread = std::thread::Builder::new()
            .name("focal-test-relay".into())
            .spawn(move || {
                run(
                    front_socket,
                    back,
                    delay,
                    jitter,
                    bits,
                    &stopping,
                    &counting,
                )
            })
            .unwrap();
        Self {
            front,
            stop,
            carried,
            thread: Some(thread),
        }
    }
    pub fn front(&self) -> SocketAddr {
        self.front
    }
    /// The bytes carried toward the node behind the relay so far: what its
    /// peers and clients sent it, datagram by datagram, losses excepted.
    pub fn carried_toward_back(&self) -> u64 {
        self.carried[0].load(Ordering::Acquire)
    }
    /// The bytes carried from the node toward its clients so far.
    pub fn carried_toward_front(&self) -> u64 {
        self.carried[1].load(Ordering::Acquire)
    }
    /// The datagrams dropped at the full queue toward the node, and toward
    /// its clients.
    pub fn dropped(&self) -> (u64, u64) {
        (
            self.carried[2].load(Ordering::Acquire),
            self.carried[3].load(Ordering::Acquire),
        )
    }
}
impl Drop for Relay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// One client the relay has seen: the socket the node behind sees it as.
struct Client {
    socket: UdpSocket,
    last: Instant,
}

const QUEUE: usize = 32;

fn run(
    front: UdpSocket,
    back: SocketAddr,
    delay: Duration,
    jitter: Duration,
    bits: Option<u64>,
    stop: &AtomicBool,
    carried: &[AtomicU64; 4],
) {
    let mut clients: Vec<(SocketAddr, Client)> = Vec::new();
    let mut travelling = BinaryHeap::new();
    let mut serial = 0u64;
    // When the bottleneck each way is next free, and how many wait at it.
    let mut free = [Instant::now(), Instant::now()];
    let mut queued = [0usize, 0usize];
    let mut random = 0x9E37_79B9_7F4A_7C15u64
        ^ u64::from(
            front
                .local_addr()
                .map(|address| address.port())
                .unwrap_or(1),
        );
    let mut draw = move || {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random
    };
    let mut buffer = vec![0u8; DATAGRAM];
    // Datagrams taken this pass, forwarded once their time comes.
    let mut taken: VecDeque<(SocketAddr, usize, Vec<u8>)> = VecDeque::new();
    while !stop.load(Ordering::Acquire) {
        // From a client, toward the node: by that client's own socket.
        if let Ok((length, source)) = front.recv_from(&mut buffer) {
            let via = match clients.iter().position(|(address, _)| *address == source) {
                Some(via) => via,
                None => {
                    if clients.len() >= CLIENTS
                        && let Some(oldest) = clients
                            .iter()
                            .enumerate()
                            .min_by_key(|(_, (_, client))| client.last)
                            .map(|(index, _)| index)
                    {
                        clients.swap_remove(oldest);
                    }
                    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
                    socket.set_nonblocking(true).unwrap();
                    clients.push((
                        source,
                        Client {
                            socket,
                            last: Instant::now(),
                        },
                    ));
                    clients.len() - 1
                }
            };
            if let Some((_, client)) = clients.get_mut(via) {
                client.last = Instant::now();
            }
            taken.push_back((back, via, buffer[..length].to_vec()));
        }
        // From the node, toward each client: by the front.
        for (via, (address, client)) in clients.iter().enumerate() {
            while let Ok((length, _)) = client.socket.recv_from(&mut buffer) {
                taken.push_back((*address, usize::MAX - via, buffer[..length].to_vec()));
            }
        }
        while let Some((to, via, bytes)) = taken.pop_front() {
            let extra = Duration::from_nanos(draw() % (jitter.as_nanos() as u64).saturating_add(1));
            let way = usize::from(via >= usize::MAX - CLIENTS);
            let now = Instant::now();
            // At the bottleneck: behind those before it, at the rate; one
            // that finds the queue full is dropped.
            let leaves = match bits {
                Some(bits) => {
                    if queued[way] >= QUEUE {
                        if let Some(dropped) = carried.get(2 + way) {
                            dropped.fetch_add(1, Ordering::AcqRel);
                        }
                        continue;
                    }
                    let serialization =
                        Duration::from_nanos(bytes.len() as u64 * 8 * 1_000_000_000 / bits);
                    free[way] = free[way].max(now) + serialization;
                    queued[way] += 1;
                    free[way]
                }
                None => now,
            };
            serial += 1;
            travelling.push(Travelling {
                at: leaves + delay + extra,
                serial,
                to,
                via,
                bytes,
            });
        }
        let now = Instant::now();
        while travelling.peek().is_some_and(|next| next.at <= now) {
            let Some(next) = travelling.pop() else { break };
            if bits.is_some() {
                let way = usize::from(next.via >= usize::MAX - CLIENTS);
                queued[way] = queued[way].saturating_sub(1);
            }
            if next.via == usize::MAX || next.via >= usize::MAX - CLIENTS {
                if front.send_to(&next.bytes, next.to).is_ok() {
                    carried[1].fetch_add(next.bytes.len() as u64, Ordering::AcqRel);
                }
            } else if let Some((_, client)) = clients.get(next.via)
                && client.socket.send_to(&next.bytes, next.to).is_ok()
            {
                carried[0].fetch_add(next.bytes.len() as u64, Ordering::AcqRel);
            }
        }
    }
}
