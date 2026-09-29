------------------------------- MODULE FastTrack -------------------------------
(***************************************************************************)
(* focal, the fast track of its consensus core as it is built              *)
(* (docs/archictecutre/27 section 4, crates/focal-raft/src/track.rs).      *)
(*                                                                         *)
(* A member holds beside its log, at indexes above it, entries it approved *)
(* by itself: one an index, whatever proposer it came from.  It says what  *)
(* it holds, as of its term.  The log holds what a leader approved, and    *)
(* every entry of it bears the term of the leader that took it.            *)
(*                                                                         *)
(* A leader takes any entry for the next index of its log: the first it    *)
(* hears of, or one of its own.  It commits an index of its own term       *)
(*   by the classic quorum: a majority holds its log through the index;    *)
(*   by the fast quorum: the index is the one after its commit, and        *)
(*     three quarters of the members hold the entry, by themselves or      *)
(*     from the leader.                                                    *)
(*                                                                         *)
(* A member votes for a candidate whose log is at least as current as its  *)
(* own, and says with its vote what it holds by itself.  One that is       *)
(* elected takes, for every index above its log that a voter of its        *)
(* quorum holds an entry at, the entry most held among them, and for an    *)
(* index none of them holds anything at an entry that states nothing;      *)
(* then it writes its own first entry.                                     *)
(*                                                                         *)
(* A member takes from a leader what follows a point both hold.  What is   *)
(* committed at the member is taken to be what the leader holds there,     *)
(* whatever term it bears: an entry committed by the fast quorum bears the *)
(* term of the leader that took it, and the leader after it, which took it *)
(* again at its election, gave it its own.                                 *)
(*                                                                         *)
(* Messages are not lost one by one here: what was said stays said, and    *)
(* an action may act on it at any later time or never, which is every      *)
(* order, delay, repetition and loss.  A leader that was deposed and does  *)
(* not know it goes on leading its term, and a member that stopped is one  *)
(* that does nothing for a while: what is durable is all a member has      *)
(* here.  A leader's own first entry is an entry it takes like any other.  *)
(*                                                                         *)
(* To keep the states few enough to visit them all, a member's word is     *)
(* kept as the last it gave: what it last said it holds at an index, and   *)
(* what it held when it last voted.  A leader sends its log through its    *)
(* end.                                                                    *)
(*                                                                         *)
(* Checked:                                                                *)
(*   Agreement     no two members commit entries that state different     *)
(*                 things at one index                                     *)
(*   Committed     what a member has committed at an index is what was     *)
(*                 first committed there                                   *)
(*   LeaderHolds   a leader of a later term holds what was committed       *)
(*   OneLeader     no term has two leaders                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Servers,   \* the voters
          Values,    \* what proposers propose
          Noop,      \* what a leader's own first entry states
          Nothing,   \* no entry
          Nobody,    \* no vote
          MaxTerm,
          MaxLen     \* how long a log grows

Classic == {Q \in SUBSET Servers : 2 * Cardinality(Q) > Cardinality(Servers)}
Fast    == {Q \in SUBSET Servers : 4 * Cardinality(Q) >= 3 * Cardinality(Servers)}

Stated  == Values \cup {Noop}
Indexes == 1..MaxLen
Entries == [term : 1..MaxTerm, value : Stated]

VARIABLES
  term,     \* [Servers -> 0..MaxTerm]
  vote,     \* [Servers -> Servers \cup {Nobody}]
  role,     \* [Servers -> {"follower", "candidate", "leader"}]
  log,      \* [Servers -> Seq(Entries)]
  held,     \* [Servers -> [Indexes -> Values \cup {Nothing}]]
  commit,   \* [Servers -> 0..MaxLen]
  says,     \* [Servers -> [Indexes -> [term, value]]]: what a member last
            \* said it holds by itself
  acks,     \* [Servers -> [0..MaxTerm -> 0..MaxLen]]: through which index a
            \* member said it holds the log of the leader of a term
  grants,   \* [Servers -> [Indexes -> value]]: what a member held when
            \* it last voted
  chosen    \* [Indexes -> [value, term]]: what was first committed, and by a leader of which term

vars == <<term, vote, role, log, held, commit, says, acks, grants, chosen>>

NotChosen == [value |-> Nothing, term |-> 0]

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
LastTerm(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].term

\* What a member holds by itself is above its log.
Release(h, length) == [i \in Indexes |-> IF i <= length THEN Nothing ELSE h[i]]

TypeOK ==
  /\ term \in [Servers -> 0..MaxTerm]
  /\ vote \in [Servers -> Servers \cup {Nobody}]
  /\ role \in [Servers -> {"follower", "candidate", "leader"}]
  /\ \A s \in Servers : /\ Len(log[s]) <= MaxLen
                        /\ \A i \in 1..Len(log[s]) : log[s][i] \in Entries
                        /\ commit[s] <= Len(log[s])
  /\ held \in [Servers -> [Indexes -> Values \cup {Nothing}]]
  /\ \A s \in Servers : \A i \in Indexes : i <= Len(log[s]) => held[s][i] = Nothing

Init ==
  /\ term   = [s \in Servers |-> 0]
  /\ vote   = [s \in Servers |-> Nobody]
  /\ role   = [s \in Servers |-> "follower"]
  /\ log    = [s \in Servers |-> << >>]
  /\ held   = [s \in Servers |-> [i \in Indexes |-> Nothing]]
  /\ commit = [s \in Servers |-> 0]
  /\ says   = [s \in Servers |-> [i \in Indexes |-> NotChosen]]
  /\ acks   = [s \in Servers |-> [t \in 0..MaxTerm |-> 0]]
  /\ grants = [s \in Servers |-> [i \in Indexes |-> Nothing]]
  /\ chosen = [i \in Indexes |-> NotChosen]

----------------------------------------------------------------------------
\* A proposal reaches a member, which holds it if it holds nothing there.
Hold(m, i, v) ==
  /\ i > Len(log[m])
  /\ held[m][i] = Nothing
  /\ held' = [held EXCEPT ![m][i] = v]
  /\ UNCHANGED <<term, vote, role, log, commit, says, acks, grants, chosen>>

\* A member says what it holds, as of its term.
Say(m, i) ==
  /\ held[m][i] # Nothing
  /\ says' = [says EXCEPT ![m][i] = [value |-> held[m][i], term |-> term[m]]]
  /\ UNCHANGED <<term, vote, role, log, held, commit, acks, grants, chosen>>

\* A leader takes an entry for the next index of its log.
Take(l, v) ==
  /\ role[l] = "leader"
  /\ Len(log[l]) < MaxLen
  /\ log' = [log EXCEPT ![l] = Append(@, [term |-> term[l], value |-> v])]
  /\ held' = [held EXCEPT ![l] = Release(@, Len(log[l]) + 1)]
  /\ UNCHANGED <<term, vote, role, commit, says, acks, grants, chosen>>

\* Who holds the entry a leader has at an index, as the leader was told.
HoldsByItself(l, m, i) ==
  \/ says[m][i] = [value |-> log[l][i].value, term |-> term[l]]
  \/ /\ vote[m] = l /\ term[m] = term[l]
     /\ grants[m][i] = log[l][i].value
HoldsFromLeader(l, m, i) ==
  \/ m = l
  \/ acks[m][term[l]] >= i

Choose(i, l) ==
  [chosen EXCEPT ![i] = IF @ = NotChosen
                         THEN [value |-> log[l][i].value, term |-> term[l]]
                         ELSE @]

FastCommit(l) ==
  LET i == commit[l] + 1 IN
  /\ role[l] = "leader"
  /\ i <= Len(log[l])
  /\ log[l][i].term = term[l]
  /\ {m \in Servers : HoldsByItself(l, m, i) \/ HoldsFromLeader(l, m, i)} \in Fast
  /\ commit' = [commit EXCEPT ![l] = i]
  /\ chosen' = Choose(i, l)
  /\ UNCHANGED <<term, vote, role, log, held, says, acks, grants>>

ClassicCommit(l, i) ==
  /\ role[l] = "leader"
  /\ i > commit[l]
  /\ i <= Len(log[l])
  /\ log[l][i].term = term[l]
  /\ {m \in Servers : HoldsFromLeader(l, m, i)} \in Classic
  /\ commit' = [commit EXCEPT ![l] = i]
  /\ chosen' = [j \in Indexes |->
                 IF j > commit[l] /\ j <= i /\ chosen[j] = NotChosen
                 THEN [value |-> log[l][j].value, term |-> term[l]]
                 ELSE chosen[j]]
  /\ UNCHANGED <<term, vote, role, log, held, says, acks, grants>>

\* A member takes from a leader what follows the point p, through k.
Replicate(l, m, p) ==
  LET k == Len(log[l]) IN
  /\ l # m
  /\ role[l] = "leader"
  /\ term[m] <= term[l]
  /\ p <= k
  /\ \/ p = 0
     \/ p <= commit[m]
     \/ p >= 1 /\ p <= Len(log[m]) /\ log[m][p].term = log[l][p].term
  /\ LET from     == Max(p, commit[m]) + 1
         differs  == {i \in from..k : i > Len(log[m]) \/ log[m][i].term # log[l][i].term}
         taken    == IF differs = {}
                     THEN log[m]
                     ELSE LET c == CHOOSE i \in differs : \A j \in differs : i <= j
                          IN SubSeq(log[m], 1, c - 1) \o SubSeq(log[l], c, k)
     IN /\ log' = [log EXCEPT ![m] = taken]
        /\ held' = [held EXCEPT ![m] = Release(@, Len(taken))]
        /\ commit' = [commit EXCEPT ![m] = Max(@, Min(commit[l], k))]
  /\ term' = [term EXCEPT ![m] = term[l]]
  /\ vote' = [vote EXCEPT ![m] = IF term[m] = term[l] THEN @ ELSE Nobody]
  /\ role' = [role EXCEPT ![m] = "follower"]
  /\ acks' = [acks EXCEPT ![m][term[l]] = Max(@, k)]
  /\ UNCHANGED <<says, grants, chosen>>

Campaign(c) ==
  /\ role[c] # "leader"
  /\ term[c] < MaxTerm
  /\ term' = [term EXCEPT ![c] = @ + 1]
  /\ vote' = [vote EXCEPT ![c] = c]
  /\ role' = [role EXCEPT ![c] = "candidate"]
  /\ grants' = [grants EXCEPT ![c] = held[c]]
  /\ UNCHANGED <<log, held, commit, says, acks, chosen>>

Current(c, m) ==
  \/ LastTerm(log[c]) > LastTerm(log[m])
  \/ LastTerm(log[c]) = LastTerm(log[m]) /\ Len(log[c]) >= Len(log[m])

Grant(m, c) ==
  /\ m # c
  /\ role[c] = "candidate"
  /\ term[m] <= term[c]
  /\ term[m] < term[c] \/ vote[m] \in {Nobody, c}
  /\ Current(c, m)
  /\ term' = [term EXCEPT ![m] = term[c]]
  /\ vote' = [vote EXCEPT ![m] = c]
  /\ role' = [role EXCEPT ![m] = "follower"]
  /\ grants' = [grants EXCEPT ![m] = held[m]]
  /\ UNCHANGED <<log, held, commit, says, acks, chosen>>

\* How many of the voters Q said they hold v at i, by their votes for c.
Count(c, Q, i, v) ==
  Cardinality({m \in Q : grants[m][i] = v})
\* MostHeld is what the core does.  LeastHeld and AnyHeld are what it does
\* not, kept to show that the properties fail without the rule.
MostHeld(c, Q, i, v) ==
  /\ Count(c, Q, i, v) > 0
  /\ \A w \in Values : Count(c, Q, i, w) <= Count(c, Q, i, v)
LeastHeld(c, Q, i, v) ==
  /\ Count(c, Q, i, v) > 0
  /\ \A w \in Values : Count(c, Q, i, w) > 0 => Count(c, Q, i, v) <= Count(c, Q, i, w)

CONSTANT Rule   \* "most" | "least"
Recovered(c, Q, i, v) ==
  IF \A w \in Values : Count(c, Q, i, w) = 0
  THEN v = Noop
  ELSE IF Rule = "most" THEN MostHeld(c, Q, i, v) ELSE LeastHeld(c, Q, i, v)

Lead(c, Q) ==
  /\ role[c] = "candidate"
  /\ Q \in Classic
  /\ \A m \in Q : vote[m] = c /\ term[m] = term[c]
  /\ LET length == Len(log[c])
         reported == {i \in Indexes : /\ i > length
                                      /\ \E v \in Values : Count(c, Q, i, v) > 0}
         top == IF reported = {} THEN length
                ELSE CHOOSE i \in reported : \A j \in reported : j <= i
     IN \E taken \in [(length + 1)..top -> Stated] :
             /\ \A i \in (length + 1)..top : Recovered(c, Q, i, taken[i])
             /\ log' = [log EXCEPT ![c] =
                          @ \o [i \in 1..(top - length) |->
                                 [term |-> term[c], value |-> taken[length + i]]]]
             /\ held' = [held EXCEPT ![c] = Release(@, top)]
  /\ role' = [role EXCEPT ![c] = "leader"]
  /\ UNCHANGED <<term, vote, commit, says, acks, grants, chosen>>

Next ==
  \/ \E m \in Servers, i \in Indexes, v \in Values : Hold(m, i, v)
  \/ \E m \in Servers, i \in Indexes : Say(m, i)
  \/ \E l \in Servers, v \in Stated : Take(l, v)
  \/ \E l \in Servers : FastCommit(l)
  \/ \E l \in Servers, i \in Indexes : ClassicCommit(l, i)
  \/ \E l, m \in Servers, p \in 0..MaxLen : Replicate(l, m, p)
  \/ \E c \in Servers : Campaign(c)
  \/ \E m, c \in Servers : Grant(m, c)
  \/ \E c \in Servers, Q \in SUBSET Servers : Lead(c, Q)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
Agreement ==
  \A m, n \in Servers : \A i \in 1..Min(commit[m], commit[n]) :
    log[m][i].value = log[n][i].value

Committed ==
  \A m \in Servers : \A i \in 1..commit[m] :
    /\ chosen[i] # NotChosen
    /\ log[m][i].value = chosen[i].value

LeaderHolds ==
  \A l \in Servers : role[l] = "leader" =>
    \A i \in Indexes : (chosen[i] # NotChosen /\ chosen[i].term < term[l]) =>
      /\ i <= Len(log[l])
      /\ log[l][i].value = chosen[i].value

OneLeader ==
  \A l, m \in Servers :
    (role[l] = "leader" /\ role[m] = "leader" /\ term[l] = term[m]) => l = m

\* For the checker: the servers are alike.
Alike == Permutations(Servers) \cup Permutations(Values)
=============================================================================
