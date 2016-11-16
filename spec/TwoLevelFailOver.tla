--------------------------- MODULE TwoLevelFailOver ---------------------------

\* The replication and fail-over protocol for Exabyte Scavenger. This is
\* essentially Vertical Paxos
\*
\* Vertical Paxos:
\* http://research.microsoft.com/en-us/um/people/lamport/pubs/vertical-paxos.pdf
\*
\* The benifit of Vertical Paxos is that fail-over are mostly handled by each
\* replica instead of the manager. The manager's jobs are only supplying new
\* replicas to partitions with reduced redundency, issue new ballot. Vertical
\* Paxos does not concern fault-detection or bringing new replicas, so we
\* are free to experiment with different algorithms.
\*
\* This fits ExaVenger very well as we don't want manager to be a bottle
\* neck when scaling to one million servers.
\*
\* How quickly we want the fail-over to happen? If the time
\* threshold is too small(e.g. sub second), we will have frequent threshing.
\* If the threshold is big (e.g. 5 minutes), the partition would be unavailable
\* when we try to decide whether a non-responding replica is dead or not.
\*
\* We can set the threshold to be very small (200 ms) to induce frequent 
\* tentative fail-over. At the same time we try to make the tentative
\* fail-over light weight: it involves a couple of message exchanges to
\* the manager to decide the new tentative primary, and a replay of the
\* new primary's preparation queue. We do not do HDD to SSD promotion or
\* transfer of partition data to a new machine at this point.
\*
\* When the timeout is 200ms, assuming 20 updates per second per partition,
\* the preparation queue only has a handful of items. So the tentative
\* fail-over should be cheap.
\*
\* Now the permanent fail-over
\*
\* The primary partition, if it's data is on HDD, would always monitor the
\* inactive replica whose data is on SSD. If within 5 minutes it comes back
\* and claiming to have all the data, the primary status would
\* transfer back.  Or else the primary would ask the manager to mark the SSD
\* replica as dead or corrupted, and performs HDD to SSD transfer on itself.
\*
\* The manager is responsible to bring new machines to partitions with reduced
\* redundency for 5 minutes. It would send a "kill" command to a machine with
\* many dead replicas.
\* 
\* The only change to the protocol is that a primary would promote an inactive
\* replica to primary.

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANT TotalReplicas,
         MaxItem,
         Interval, \* for sending beacons
         MsgDelay \* network msgs are to be delivered within this time
         
Lease == 2 * Interval
Grace == 1 * Interval
ReallyLong == 10 * Interval

Replicas == 1 .. TotalReplicas

\* We use a set to simulate network communication. Each message is a record
\* {src, dst, type, value, timer}. 
\* Here timer is an internal field to ensure the message does not stay in
\* network forever.  
\* PrepareMsg support broardcast, the destination parameter is a set. 
PrepareMsg(from, toSet, t, v) == 
    [src: {from}, dst: toSet, type: {t}, value: {v}, timer: {MsgDelay}]

\* seq is a set of integers, SeqMax is the max consequtive value in the set
SeqMax(seq) == CHOOSE i \in 0 .. MaxItem + 2: 
                 (\A smaller \in 1..i : smaller \in (DOMAIN seq))  
                 /\ ~((i+1) \in DOMAIN seq) 

Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a > b THEN b ELSE a
                   
(*****************************************************************************
--algorithm FailOverProtocol 
{
    variables
        channel = {};
        newItem = 1;
        clientNotified = 0;
        clock = 0;
        timer = [ n \in Replicas |-> Interval];
        frozenReplica = 0; \* there can be only one machine frozen at a time
  
  \*
  \* Partition configuration has 3 fields
  \* b:  ballot
  \* p:  primary of the current con
  \* alive: array indicating whether a replica is alive and up to date.
  \*
  process (ReplicaProc \in Replicas)
    variables 
        rcvTime = [ j \in Replicas |-> 0 ];
        remoteDecree = [ j \in Replicas |-> 0];
        membership = 
            [ b |-> 1, \* current ballot
              p |-> 1, \* primary replica
              alive |-> [j \in Replicas |-> TRUE], \* live replicas and dead ones
              completeBal |-> 0 ]; \* prev ballot
        pendingPrimary = FALSE;
        store = (0 :> <<0,0>>); \* decree :> <<ballot, value>> 
        committed = 0;
        pendingConsolidatePoint = 0;
  {
    l_replica:
      while (newItem <= MaxItem) {
          either {
              \* a frozen replica has the option to drop all messages
              \* and ignore timer events
              await frozenReplica = self;
              if (\E m \in channel : m.dst = self){
                  with (msg \in {m \in channel : m.dst = self}){
                      channel := channel \ {msg};
                  };
              };
              timer[self] := 0;
          }
          or {
              \* Process Manager Command
              await (\E m \in channel : m.src = 0 /\ m.dst = self);
              with (msg \in { m \in channel : m.src=0 /\ m.dst = self}){
                  assert msg.type \in {"newBallot", "activated"};
                  if (msg.type = "newBallot"){
                      if (msg.value.b > membership.b){
                          \* new partition config
                          assert msg.value.p = self;
                          
                          if ( membership.p /= self){
                              \* this is the new primary, and we need
                              \* reconsiliation, replay prepare queue
                              if (SeqMax(store) > committed){
                                  remoteDecree := [ j \in Replicas |-> Min(remoteDecree[j], committed)];
                                  with (values = { [ decree |-> d, 
                                      ballot |-> msg.value.b,
                                      value |-> store[d][2],
                                      membership |-> msg.value,
                                      commit |-> committed,
                                      time |-> clock ] 
                                      : d \in committed+1 .. SeqMax(store)} )
                                  {          
                                      channel := ( channel \ {msg} ) \cup
                                          [ src: {self}, 
                                            dst: Replicas \ {self}, 
                                            type: {"PREPARE"}, 
                                            value: values, 
                                            timer: {MsgDelay}
                                          ];
                                  };
                                  \* update ballot numbers
                                  store := [ i \in { d \in DOMAIN store: d <= SeqMax(store)}  
                                     |-> IF i \in committed+1 .. SeqMax(store) THEN <<msg.value.b, store[i][2]>> ELSE store[i] ];
                              }
                              else {
                                  channel := ( channel \ {msg} ) \cup
                                          [ src: {self}, 
                                            dst: Replicas \ {self}, 
                                            type: {"PREPARE"}, 
                                            value: [ decree : {SeqMax(store)},
                                                     ballot : {store[SeqMax(store)][1]}, \* use old ballot here, this is just broadcast new config 
                                                     value : {store[SeqMax(store)][2]},
                                                     membership : {msg.value},
                                                     commit : {committed},
                                                     time : {clock} 
                                                   ], 
                                            timer: {MsgDelay}
                                          ]
                                          \cup PrepareMsg(self, {0}, "complete", msg.value);
                                   store := [ decree \in { d \in DOMAIN store: d <= SeqMax(store)} 
                                       |-> store[decree] ];
                              };
                              pendingConsolidatePoint := SeqMax(store);              
                          }
                          else {
                              \* no reconsiliation
                              channel := ( channel \ {msg}) \cup
                                  PrepareMsg(self, {0}, "complete", msg.value);
                              pendingConsolidatePoint := committed;
                          };
                          pendingPrimary := TRUE;
                          membership := msg.value;
                          timer[self]:= Interval;
    
                          \* make sure we don't lose data during fail-over
                          assert SeqMax(store) >= clientNotified;
                          assert clientNotified >= committed;

                          \* it should be ok to override other replicas items
                          \* on their prepare queue (not committed), for reasons:
                          \* 1. not "committed" means it is not a successful decree yet
                          \* 2. this new replica would have a higher ballot, that would
                          \*    win according to B3 in "the part time parliament".
                      }
                      else {
                          \* ignore command with old config
                          channel := channel \ {msg};
                      };
                  } else {
                      \* Received "activated"
                      if (msg.value = membership.b){
                          assert membership.p = self;
                          pendingPrimary := FALSE;
                      };
                      channel := channel \ {msg};
                  }
                   
              }
          }
          or {
              \* Received "PREPARE"
              await (\E m \in channel : m.dst = self /\ m.src \in Replicas /\ m.type = "PREPARE");
              with (msg \in { m \in channel : m.dst = self /\ m.src \in Replicas /\ m.type = "PREPARE"}){
                  if ( membership.b <= msg.value.membership.b ){
                      assert msg.value.membership.b >= msg.value.ballot;
                      assert msg.value.membership.p /= self;
                      assert msg.value.membership.p = msg.src;

                      \* make sure values with bigger ballot are preserved.
                      assert IF msg.value.decree \in DOMAIN store 
                                /\ store[msg.value.decree][1] >= msg.value.ballot
                             THEN store[msg.value.decree][2] = msg.value.value
                             ELSE TRUE; 

                      if (membership.b < msg.value.membership.b /\ membership.p # msg.value.membership.p) {
                          \* new primary, consolidation in progress
                          if (msg.value.membership.alive[self]){
                              \* this is secondary, we need to remove all bigger decree numbers
                              \* from prepare queue, in case the new primary's biggest prepared decree is
                              \* smaller than ours, it may reuse that decree number for other values.
                              \* we should not need to update already committed one, as the new primary's
                              \* biggest decree should be no smaller than our committed decree.
                              with (toRemove = { d \in DOMAIN store: d >= Max(committed+1, msg.value.decree) }) {
                                  store :=
                                       [ d \in (DOMAIN store \ toRemove) \cup {msg.value.decree} |->
                                           IF d = msg.value.decree /\ (d > committed \/ ~(d \in DOMAIN store))
                                           THEN <<msg.value.ballot, msg.value.value>>
                                           ELSE store[d]
                                       ];
                              }
                          }
                          else {
                              \* this is inactive, we don't know when did the reconfig happen, but definatedly
                              \* after we last updated "committed" or lost a decree.
                              \* so we need to remove all decrees in the prepare queue
                              with (toRemove = { d \in DOMAIN store: d > Min(committed, SeqMax(store)) }) {
                                  store :=
                                       [ d \in (DOMAIN store \ toRemove) \cup {msg.value.decree} |->
                                           IF d = msg.value.decree /\ d > Min(committed, SeqMax(store)) 
                                           THEN <<msg.value.ballot, msg.value.value>>
                                           ELSE store[d]
                                       ];
                              }
                          };
                      } else if (msg.value.decree > Min(committed, SeqMax(store))){
                          store := store @@ (msg.value.decree :> <<msg.value.ballot, msg.value.value>>);
                      };

                      \* commit prepare request to store
                      committed := Max(committed, msg.value.commit); 
                      membership := msg.value.membership;

                      channel := (channel \ {msg}) \cup 
                          PrepareMsg(self, {msg.src}, "ACK", 
                              [decree |-> Min(SeqMax(store), msg.value.decree), time |-> msg.value.time]);
                              
                      rcvTime[msg.src] := clock;
                  }
                  else {
                      \* ignore msg with old config,
                      \* maybe send NACK? 
                      channel := channel \ {msg};
                  };
              }; 
          }
          or {
              \* PRIMARY receive an ACK from replica
              await (\E m \in channel : m.dst = self /\ m.src \in Replicas /\ m.type = "ACK");
              with (msg \in { m \in channel : m.dst = self /\ m.src \in Replicas /\ m.type = "ACK"}){
                  \* process "ACK", [decree, time]
                  \* time is the time stamp of the prepare message
                  if (membership.p = self){
                      remoteDecree[msg.src] := msg.value.decree;
                      rcvTime[msg.src] := IF remoteDecree[msg.src] < msg.value.decree 
                                          THEN msg.value.time
                                          ELSE rcvTime[msg.src]; \* this is only for inactive ones
    
                      if (\E i \in Replicas \ {membership.p}: membership.alive[i]){
                        with (secondaries = {i \in Replicas \ {membership.p}: membership.alive[i] }){
                            with (smallest \in {s \in secondaries :
                                       \A o \in secondaries : 
                                           remoteDecree[o] >= remoteDecree[s]} ){
                                \* commit those items that all secondaries have a copy
                                committed := Max(remoteDecree[smallest], committed);
                                clientNotified := Max(remoteDecree[smallest], clientNotified);
                            }
                        }
                      };
                      
                      if ( msg.value.decree >= committed
                          /\ ~membership.alive[msg.src]
                          /\ ~pendingPrimary 
                      ){
                          \* an inactive one caught up, promote it
                          with (p = [ membership EXCEPT !.alive[msg.src] = TRUE]){
                              channel := (channel \ {msg}) \cup
                                  PrepareMsg(self, {0}, "PROMOTE_INACTIVE", p);
                              \* treat it as a secondary, so that we try to
                              \* remove it as soon as it falls behind again.
                              membership := p;
                          };
                      }
                      else if (pendingPrimary /\ committed >= pendingConsolidatePoint) {
                          channel := (channel \ {msg}) \cup
                              PrepareMsg(self, {0}, "complete", membership);
                      }
                      else {
                          channel := channel \ {msg};
                      };
                      assert committed <= SeqMax(store);
                              
                  }
                  else {
                      \* ignore if repeated ack or we are not primary any more
                      \* if a secondary keeps sending an old ack, we need to
                      \* consider it time out and remove it.               
                      channel := channel \ {msg}; 
                  };
              }
          }
          or {
              \* Primary process new req or send beacon
              \* this should happen at every "Interval" time or sooner
              \* when the process is active. or else the timer should
              \* prevent action "TICK" from enabled.
              await membership.p = self;
                  
              \* check lease
              if (\E i \in Replicas \ {membership.p}: 
                     membership.alive[i] /\ clock-rcvTime[i] > Lease
                  ){
                  with (s \in { i \in Replicas \ {membership.p}: 
                     membership.alive[i] /\ clock-rcvTime[i] > Lease }
                  ){
                      with (proposal = [ membership EXCEPT !.alive[s] = FALSE]){
                          channel := channel \cup 
                              PrepareMsg(self, {0}, "SECONDARY_TIMEOUT", proposal);
                      };
                  };
                  timer[self] := Interval;
              }
              else {
                  \* there are no expired secondary, we are safe as primary
                  either {
                      await /\ (\E i \in Replicas \ { membership.p } : membership.alive[i])
                            /\ SeqMax(store) <= committed + 1
                            /\ ~pendingPrimary 
                          ;
                      \* process new request only when we have at least one
                      \* secondary, and it is ok to lose not-acknowledged item
                      newItem := newItem + 1; 
                      store := store @@ (SeqMax(store) + 1 :> <<membership.b, newItem*10 + self>>);
                  }
                  or {
                     skip; \* sometime we don't have new update 
                  };
                  \* we need to send beacon every Interval time, whether
                  \* we have new update request or not

                  \* here to send a bunch for those inactive ones to catch up.
                  \* in reality this should be implemented as to send a subset
                  \* of prepare que (even committed data) to each replica
                  \* according to each replica's most recent decree. Note that
                  \* we should only do patch up when the replica has been
                  \* lagging for a while, maybe Interval time.
                  with (values = { [ decree |-> d, 
                                    ballot |-> store[d][1],
                                    value |-> store[d][2],
                                    membership |-> membership,
                                    commit |-> committed,
                                    time |-> clock ] 
                                  : d \in Min(committed+1, SeqMax(store)) .. SeqMax(store)} ){
                      channel := channel \cup
                          [ src: {self}, 
                            dst: Replicas \ {self}, 
                            type: {"PREPARE"}, 
                            value: values, 
                            timer: {MsgDelay}
                          ]
                          \cup IF (pendingPrimary /\ committed >= pendingConsolidatePoint)
                               THEN  PrepareMsg(self, {0}, "complete", membership)
                               ELSE {};
                  };
                  timer[self] := Interval;
              }
          }
          or {
              await /\ timer[self] < 0
                    /\ membership.alive[self]
                    /\ membership.p # self; 
              \* being a secondary, check for primary lease expiration
              if ((clock - rcvTime[membership.p]) > (Lease + Grace)){
                  with (p = [ membership EXCEPT 
                              !.alive[membership.p] = FALSE, 
                              !.p = self]){
                      channel := channel \cup
                          PrepareMsg(self, {0}, "PRIMARY_TIMEOUT", p);
                  };
                  timer[self] := Interval;
              }
              else {
                  timer[self] := (Lease + Grace) - (clock - rcvTime[membership.p]);
              }
          }
          or {
              await ~membership.alive[self];
              \* INACTIVE replicas can not kick out primary
              \* just allow the tick to continue;
              timer[self] := 0;
          }; \* end either or
      }; \* end while loop
  }
  
  
  process (Manager = 0)
    variable 
        curConfig = [ b |-> 1, p |-> 1, alive |-> [j \in Replicas |-> TRUE], completeBal |-> 1];
        completeConfig = [ b |-> 1, p |-> 1, alive |-> [j \in Replicas |-> TRUE], completeBal |-> 1];
        nextBallot = 2;
  {
    l_manager:
      while (clientNotified <= MaxItem) {
          either {
              \* manager could change primary anytime for any reason. 
              \* this opens the door for switch back to SSD replica
              \* or manual operator command
              with (newp \in {i \in {3}: \* can be anyone in Replicas
                      /\ i # curConfig.p
                      /\ curConfig.p = completeConfig.p
                      /\ completeConfig.alive[i]
                      /\ curConfig.alive[i]} ){
                  
                  curConfig := [curConfig EXCEPT 
                                   !.b = nextBallot,
                                   !.completeBal = completeConfig.b,
                                   !.p = newp ];
                  channel := channel  
                       \cup PrepareMsg(0, {curConfig.p}, "newBallot", curConfig);
                  nextBallot := nextBallot + 1;
              }
              
          }
          or {
              await (\E m \in channel : m.dst = 0);
              with (req \in { i \in channel : i.dst = 0 }){
                  if (req.type = "complete"){
                      if (req.value.completeBal = completeConfig.b) {
                          \*assert req.value = curConfig;
                          \* curConfig.completeBal := req.value.b;
                          completeConfig := req.value;
                          channel := (channel \ {req}) \cup
                              PrepareMsg(0, {req.value.p}, "activated", req.value.b);
                      } else {
                          channel := channel \ {req};
                      };
                  }
                  else if (completeConfig.alive[req.value.p]){ 
                      assert req.value.b <= curConfig.b;
                      assert req.type \in {"PRIMARY_TIMEOUT","SECONDARY_TIMEOUT","PROMOTE_INACTIVE"};

                      \* In production we may want to reject a primary
                      \* changing request if we just had one.                      
                      curConfig := [req.value EXCEPT 
                                  !.b = nextBallot,
                                  !.completeBal = completeConfig.b ];

                      channel := (channel \ {req}) 
                             \cup PrepareMsg(0, {curConfig.p}, "newBallot", curConfig);
                      nextBallot := nextBallot + 1;
                  } else {
                      channel := (channel \ {req});
                  }
              };
          };
      }  
  }
  
  process (Choas = (TotalReplicas + 1))
      variables count = 0;
  {
      \* Simulate loss of packet over network. the limit here is very ugly
      \* yet I don't know how to specify that if a message has been sent
      \* many times it would eventually be received.
      l: while (count < 10) {
          either {
              await frozenReplica = 0;
              with (i \in Replicas){
                  frozenReplica := i;
              }
          }
          or {
              await frozenReplica \in Replicas;
              frozenReplica := 0;
          }
          or {
              await channel /= {};
              with ( ms \in channel){
                  channel := channel \ {ms};
              };
              count := count +1;              
          };
      };
      
      frozenReplica := 0;
  }
  
  process (Tick = (TotalReplicas + 2))
  {
      \* Thanks Dr. Lamport for direction:
      \* http://research.microsoft.com/pubs/64632/tr-2005-30.pdf
      \* Advance time, the condition ensures messages are
      \* delivered within a time frame, and other processes
      \* can not sleep forever.
    l_tick:
      while (TRUE){
          with (d \in 1..Lease+Grace){
              await 
                  /\ (\A n \in Replicas :
                      timer[n] + 1 >= d )
                  /\ (\A ms \in channel: ms.timer >= d)
              ;
              clock := clock + d;
              timer := [n \in Replicas |-> timer[n] - d];
              channel := LET F(ms) == [ms EXCEPT !.timer = ms.timer - d]
                         IN {F(x) : x \in channel }
          } 
      }
  }
  
}
******************************************************************************)
\* BEGIN TRANSLATION
VARIABLES channel, newItem, clientNotified, clock, timer, frozenReplica, pc, 
          rcvTime, remoteDecree, membership, pendingPrimary, store, committed, 
          pendingConsolidatePoint, curConfig, completeConfig, nextBallot, 
          count

vars == << channel, newItem, clientNotified, clock, timer, frozenReplica, pc, 
           rcvTime, remoteDecree, membership, pendingPrimary, store, 
           committed, pendingConsolidatePoint, curConfig, completeConfig, 
           nextBallot, count >>

ProcSet == (Replicas) \cup {0} \cup {(TotalReplicas + 1)} \cup {(TotalReplicas + 2)}

Init == (* Global variables *)
        /\ channel = {}
        /\ newItem = 1
        /\ clientNotified = 0
        /\ clock = 0
        /\ timer = [ n \in Replicas |-> Interval]
        /\ frozenReplica = 0
        (* Process ReplicaProc *)
        /\ rcvTime = [self \in Replicas |-> [ j \in Replicas |-> 0 ]]
        /\ remoteDecree = [self \in Replicas |-> [ j \in Replicas |-> 0]]
        /\ membership = [self \in Replicas |-> [ b |-> 1,
                                                 p |-> 1,
                                                 alive |-> [j \in Replicas |-> TRUE],
                                                 completeBal |-> 0 ]]
        /\ pendingPrimary = [self \in Replicas |-> FALSE]
        /\ store = [self \in Replicas |-> (0 :> <<0,0>>)]
        /\ committed = [self \in Replicas |-> 0]
        /\ pendingConsolidatePoint = [self \in Replicas |-> 0]
        (* Process Manager *)
        /\ curConfig = [ b |-> 1, p |-> 1, alive |-> [j \in Replicas |-> TRUE], completeBal |-> 1]
        /\ completeConfig = [ b |-> 1, p |-> 1, alive |-> [j \in Replicas |-> TRUE], completeBal |-> 1]
        /\ nextBallot = 2
        (* Process Choas *)
        /\ count = 0
        /\ pc = [self \in ProcSet |-> CASE self \in Replicas -> "l_replica"
                                        [] self = 0 -> "l_manager"
                                        [] self = (TotalReplicas + 1) -> "l"
                                        [] self = (TotalReplicas + 2) -> "l_tick"]

l_replica(self) == /\ pc[self] = "l_replica"
                   /\ IF newItem <= MaxItem
                         THEN /\ \/ /\ frozenReplica = self
                                    /\ IF \E m \in channel : m.dst = self
                                          THEN /\ \E msg \in {m \in channel : m.dst = self}:
                                                    channel' = channel \ {msg}
                                          ELSE /\ TRUE
                                               /\ UNCHANGED channel
                                    /\ timer' = [timer EXCEPT ![self] = 0]
                                    /\ UNCHANGED <<newItem, clientNotified, rcvTime, remoteDecree, membership, pendingPrimary, store, committed, pendingConsolidatePoint>>
                                 \/ /\ (\E m \in channel : m.src = 0 /\ m.dst = self)
                                    /\ \E msg \in { m \in channel : m.src=0 /\ m.dst = self}:
                                         /\ Assert(msg.type \in {"newBallot", "activated"}, 
                                                   "Failure of assertion at line 126, column 19.")
                                         /\ IF msg.type = "newBallot"
                                               THEN /\ IF msg.value.b > membership[self].b
                                                          THEN /\ Assert(msg.value.p = self, 
                                                                         "Failure of assertion at line 130, column 27.")
                                                               /\ IF membership[self].p /= self
                                                                     THEN /\ IF SeqMax(store[self]) > committed[self]
                                                                                THEN /\ remoteDecree' = [remoteDecree EXCEPT ![self] = [ j \in Replicas |-> Min(remoteDecree[self][j], committed[self])]]
                                                                                     /\ LET values ==            { [ decree |-> d,
                                                                                                      ballot |-> msg.value.b,
                                                                                                      value |-> store[self][d][2],
                                                                                                      membership |-> msg.value,
                                                                                                      commit |-> committed[self],
                                                                                                      time |-> clock ]
                                                                                                      : d \in committed[self]+1 .. SeqMax(store[self])} IN
                                                                                          channel' = (       ( channel \ {msg} ) \cup
                                                                                                      [ src: {self},
                                                                                                        dst: Replicas \ {self},
                                                                                                        type: {"PREPARE"},
                                                                                                        value: values,
                                                                                                        timer: {MsgDelay}
                                                                                                      ])
                                                                                     /\ store' = [store EXCEPT ![self] =       [ i \in { d \in DOMAIN store[self]: d <= SeqMax(store[self])}
                                                                                                                         |-> IF i \in committed[self]+1 .. SeqMax(store[self]) THEN <<msg.value.b, store[self][i][2]>> ELSE store[self][i] ]]
                                                                                ELSE /\ channel' = (   ( channel \ {msg} ) \cup
                                                                                                    [ src: {self},
                                                                                                      dst: Replicas \ {self},
                                                                                                      type: {"PREPARE"},
                                                                                                      value: [ decree : {SeqMax(store[self])},
                                                                                                               ballot : {store[self][SeqMax(store[self])][1]},
                                                                                                               value : {store[self][SeqMax(store[self])][2]},
                                                                                                               membership : {msg.value},
                                                                                                               commit : {committed[self]},
                                                                                                               time : {clock}
                                                                                                             ],
                                                                                                      timer: {MsgDelay}
                                                                                                    ]
                                                                                                    \cup PrepareMsg(self, {0}, "complete", msg.value))
                                                                                     /\ store' = [store EXCEPT ![self] =      [ decree \in { d \in DOMAIN store[self]: d <= SeqMax(store[self])}
                                                                                                                         |-> store[self][decree] ]]
                                                                                     /\ UNCHANGED remoteDecree
                                                                          /\ pendingConsolidatePoint' = [pendingConsolidatePoint EXCEPT ![self] = SeqMax(store'[self])]
                                                                     ELSE /\ channel' = (       ( channel \ {msg}) \cup
                                                                                         PrepareMsg(self, {0}, "complete", msg.value))
                                                                          /\ pendingConsolidatePoint' = [pendingConsolidatePoint EXCEPT ![self] = committed[self]]
                                                                          /\ UNCHANGED << remoteDecree, 
                                                                                          store >>
                                                               /\ pendingPrimary' = [pendingPrimary EXCEPT ![self] = TRUE]
                                                               /\ membership' = [membership EXCEPT ![self] = msg.value]
                                                               /\ timer' = [timer EXCEPT ![self] = Interval]
                                                               /\ Assert(SeqMax(store'[self]) >= clientNotified, 
                                                                         "Failure of assertion at line 188, column 27.")
                                                               /\ Assert(clientNotified >= committed[self], 
                                                                         "Failure of assertion at line 189, column 27.")
                                                          ELSE /\ channel' = channel \ {msg}
                                                               /\ UNCHANGED << timer, 
                                                                               remoteDecree, 
                                                                               membership, 
                                                                               pendingPrimary, 
                                                                               store, 
                                                                               pendingConsolidatePoint >>
                                               ELSE /\ IF msg.value = membership[self].b
                                                          THEN /\ Assert(membership[self].p = self, 
                                                                         "Failure of assertion at line 204, column 27.")
                                                               /\ pendingPrimary' = [pendingPrimary EXCEPT ![self] = FALSE]
                                                          ELSE /\ TRUE
                                                               /\ UNCHANGED pendingPrimary
                                                    /\ channel' = channel \ {msg}
                                                    /\ UNCHANGED << timer, 
                                                                    remoteDecree, 
                                                                    membership, 
                                                                    store, 
                                                                    pendingConsolidatePoint >>
                                    /\ UNCHANGED <<newItem, clientNotified, rcvTime, committed>>
                                 \/ /\ (\E m \in channel : m.dst = self /\ m.src \in Replicas /\ m.type = "PREPARE")
                                    /\ \E msg \in { m \in channel : m.dst = self /\ m.src \in Replicas /\ m.type = "PREPARE"}:
                                         IF membership[self].b <= msg.value.membership.b
                                            THEN /\ Assert(msg.value.membership.b >= msg.value.ballot, 
                                                           "Failure of assertion at line 217, column 23.")
                                                 /\ Assert(msg.value.membership.p /= self, 
                                                           "Failure of assertion at line 218, column 23.")
                                                 /\ Assert(msg.value.membership.p = msg.src, 
                                                           "Failure of assertion at line 219, column 23.")
                                                 /\ Assert(IF msg.value.decree \in DOMAIN store[self]
                                                              /\ store[self][msg.value.decree][1] >= msg.value.ballot
                                                           THEN store[self][msg.value.decree][2] = msg.value.value
                                                           ELSE TRUE, 
                                                           "Failure of assertion at line 222, column 23.")
                                                 /\ IF membership[self].b < msg.value.membership.b /\ membership[self].p # msg.value.membership.p
                                                       THEN /\ IF msg.value.membership.alive[self]
                                                                  THEN /\ LET toRemove == { d \in DOMAIN store[self]: d >= Max(committed[self]+1, msg.value.decree) } IN
                                                                            store' = [store EXCEPT ![self] = [ d \in (DOMAIN store[self] \ toRemove) \cup {msg.value.decree} |->
                                                                                                                 IF d = msg.value.decree /\ (d > committed[self] \/ ~(d \in DOMAIN store[self]))
                                                                                                                 THEN <<msg.value.ballot, msg.value.value>>
                                                                                                                 ELSE store[self][d]
                                                                                                             ]]
                                                                  ELSE /\ LET toRemove == { d \in DOMAIN store[self]: d > Min(committed[self], SeqMax(store[self])) } IN
                                                                            store' = [store EXCEPT ![self] = [ d \in (DOMAIN store[self] \ toRemove) \cup {msg.value.decree} |->
                                                                                                                 IF d = msg.value.decree /\ d > Min(committed[self], SeqMax(store[self]))
                                                                                                                 THEN <<msg.value.ballot, msg.value.value>>
                                                                                                                 ELSE store[self][d]
                                                                                                             ]]
                                                       ELSE /\ IF msg.value.decree > Min(committed[self], SeqMax(store[self]))
                                                                  THEN /\ store' = [store EXCEPT ![self] = store[self] @@ (msg.value.decree :> <<msg.value.ballot, msg.value.value>>)]
                                                                  ELSE /\ TRUE
                                                                       /\ store' = store
                                                 /\ committed' = [committed EXCEPT ![self] = Max(committed[self], msg.value.commit)]
                                                 /\ membership' = [membership EXCEPT ![self] = msg.value.membership]
                                                 /\ channel' = (       (channel \ {msg}) \cup
                                                                PrepareMsg(self, {msg.src}, "ACK",
                                                                    [decree |-> Min(SeqMax(store'[self]), msg.value.decree), time |-> msg.value.time]))
                                                 /\ rcvTime' = [rcvTime EXCEPT ![self][msg.src] = clock]
                                            ELSE /\ channel' = channel \ {msg}
                                                 /\ UNCHANGED << rcvTime, 
                                                                 membership, 
                                                                 store, 
                                                                 committed >>
                                    /\ UNCHANGED <<newItem, clientNotified, timer, remoteDecree, pendingPrimary, pendingConsolidatePoint>>
                                 \/ /\ (\E m \in channel : m.dst = self /\ m.src \in Replicas /\ m.type = "ACK")
                                    /\ \E msg \in { m \in channel : m.dst = self /\ m.src \in Replicas /\ m.type = "ACK"}:
                                         IF membership[self].p = self
                                            THEN /\ remoteDecree' = [remoteDecree EXCEPT ![self][msg.src] = msg.value.decree]
                                                 /\ rcvTime' = [rcvTime EXCEPT ![self][msg.src] = IF remoteDecree'[self][msg.src] < msg.value.decree
                                                                                                  THEN msg.value.time
                                                                                                  ELSE rcvTime[self][msg.src]]
                                                 /\ IF \E i \in Replicas \ {membership[self].p}: membership[self].alive[i]
                                                       THEN /\ LET secondaries == {i \in Replicas \ {membership[self].p}: membership[self].alive[i] } IN
                                                                 \E smallest \in         {s \in secondaries :
                                                                                 \A o \in secondaries :
                                                                                     remoteDecree'[self][o] >= remoteDecree'[self][s]}:
                                                                   /\ committed' = [committed EXCEPT ![self] = Max(remoteDecree'[self][smallest], committed[self])]
                                                                   /\ clientNotified' = Max(remoteDecree'[self][smallest], clientNotified)
                                                       ELSE /\ TRUE
                                                            /\ UNCHANGED << clientNotified, 
                                                                            committed >>
                                                 /\ IF  msg.value.decree >= committed'[self]
                                                       /\ ~membership[self].alive[msg.src]
                                                       /\ ~pendingPrimary[self]
                                                       THEN /\ LET p == [ membership[self] EXCEPT !.alive[msg.src] = TRUE] IN
                                                                 /\ channel' = (       (channel \ {msg}) \cup
                                                                                PrepareMsg(self, {0}, "PROMOTE_INACTIVE", p))
                                                                 /\ membership' = [membership EXCEPT ![self] = p]
                                                       ELSE /\ IF pendingPrimary[self] /\ committed'[self] >= pendingConsolidatePoint[self]
                                                                  THEN /\ channel' = (       (channel \ {msg}) \cup
                                                                                      PrepareMsg(self, {0}, "complete", membership[self]))
                                                                  ELSE /\ channel' = channel \ {msg}
                                                            /\ UNCHANGED membership
                                                 /\ Assert(committed'[self] <= SeqMax(store[self]), 
                                                           "Failure of assertion at line 322, column 23.")
                                            ELSE /\ channel' = channel \ {msg}
                                                 /\ UNCHANGED << clientNotified, 
                                                                 rcvTime, 
                                                                 remoteDecree, 
                                                                 membership, 
                                                                 committed >>
                                    /\ UNCHANGED <<newItem, timer, pendingPrimary, store, pendingConsolidatePoint>>
                                 \/ /\ membership[self].p = self
                                    /\ IF \E i \in Replicas \ {membership[self].p}:
                                             membership[self].alive[i] /\ clock-rcvTime[self][i] > Lease
                                          THEN /\ \E s \in          { i \in Replicas \ {membership[self].p}:
                                                           membership[self].alive[i] /\ clock-rcvTime[self][i] > Lease }:
                                                    LET proposal == [ membership[self] EXCEPT !.alive[s] = FALSE] IN
                                                      channel' = (       channel \cup
                                                                  PrepareMsg(self, {0}, "SECONDARY_TIMEOUT", proposal))
                                               /\ timer' = [timer EXCEPT ![self] = Interval]
                                               /\ UNCHANGED << newItem, store >>
                                          ELSE /\ \/ /\ /\ (\E i \in Replicas \ { membership[self].p } : membership[self].alive[i])
                                                        /\ SeqMax(store[self]) <= committed[self] + 1
                                                        /\ ~pendingPrimary[self]
                                                     /\ newItem' = newItem + 1
                                                     /\ store' = [store EXCEPT ![self] = store[self] @@ (SeqMax(store[self]) + 1 :> <<membership[self].b, newItem'*10 + self>>)]
                                                  \/ /\ TRUE
                                                     /\ UNCHANGED <<newItem, store>>
                                               /\ LET values == { [ decree |-> d,
                                                                   ballot |-> store'[self][d][1],
                                                                   value |-> store'[self][d][2],
                                                                   membership |-> membership[self],
                                                                   commit |-> committed[self],
                                                                   time |-> clock ]
                                                                 : d \in Min(committed[self]+1, SeqMax(store'[self])) .. SeqMax(store'[self])} IN
                                                    channel' = (       channel \cup
                                                                [ src: {self},
                                                                  dst: Replicas \ {self},
                                                                  type: {"PREPARE"},
                                                                  value: values,
                                                                  timer: {MsgDelay}
                                                                ]
                                                                \cup IF (pendingPrimary[self] /\ committed[self] >= pendingConsolidatePoint[self])
                                                                     THEN  PrepareMsg(self, {0}, "complete", membership[self])
                                                                     ELSE {})
                                               /\ timer' = [timer EXCEPT ![self] = Interval]
                                    /\ UNCHANGED <<clientNotified, rcvTime, remoteDecree, membership, pendingPrimary, committed, pendingConsolidatePoint>>
                                 \/ /\ /\ timer[self] < 0
                                       /\ membership[self].alive[self]
                                       /\ membership[self].p # self
                                    /\ IF (clock - rcvTime[self][membership[self].p]) > (Lease + Grace)
                                          THEN /\ LET p == [ membership[self] EXCEPT
                                                             !.alive[membership[self].p] = FALSE,
                                                             !.p = self] IN
                                                    channel' = (       channel \cup
                                                                PrepareMsg(self, {0}, "PRIMARY_TIMEOUT", p))
                                               /\ timer' = [timer EXCEPT ![self] = Interval]
                                          ELSE /\ timer' = [timer EXCEPT ![self] = (Lease + Grace) - (clock - rcvTime[self][membership[self].p])]
                                               /\ UNCHANGED channel
                                    /\ UNCHANGED <<newItem, clientNotified, rcvTime, remoteDecree, membership, pendingPrimary, store, committed, pendingConsolidatePoint>>
                                 \/ /\ ~membership[self].alive[self]
                                    /\ timer' = [timer EXCEPT ![self] = 0]
                                    /\ UNCHANGED <<channel, newItem, clientNotified, rcvTime, remoteDecree, membership, pendingPrimary, store, committed, pendingConsolidatePoint>>
                              /\ pc' = [pc EXCEPT ![self] = "l_replica"]
                         ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                              /\ UNCHANGED << channel, newItem, clientNotified, 
                                              timer, rcvTime, remoteDecree, 
                                              membership, pendingPrimary, 
                                              store, committed, 
                                              pendingConsolidatePoint >>
                   /\ UNCHANGED << clock, frozenReplica, curConfig, 
                                   completeConfig, nextBallot, count >>

ReplicaProc(self) == l_replica(self)

l_manager == /\ pc[0] = "l_manager"
             /\ IF clientNotified <= MaxItem
                   THEN /\ \/ /\ \E newp \in        {i \in {3}:
                                             /\ i # curConfig.p
                                             /\ curConfig.p = completeConfig.p
                                             /\ completeConfig.alive[i]
                                             /\ curConfig.alive[i]}:
                                   /\ curConfig' = [curConfig EXCEPT
                                                       !.b = nextBallot,
                                                       !.completeBal = completeConfig.b,
                                                       !.p = newp ]
                                   /\ channel' =       channel
                                                 \cup PrepareMsg(0, {curConfig'.p}, "newBallot", curConfig')
                                   /\ nextBallot' = nextBallot + 1
                              /\ UNCHANGED completeConfig
                           \/ /\ (\E m \in channel : m.dst = 0)
                              /\ \E req \in { i \in channel : i.dst = 0 }:
                                   IF req.type = "complete"
                                      THEN /\ IF req.value.completeBal = completeConfig.b
                                                 THEN /\ completeConfig' = req.value
                                                      /\ channel' = (       (channel \ {req}) \cup
                                                                     PrepareMsg(0, {req.value.p}, "activated", req.value.b))
                                                 ELSE /\ channel' = channel \ {req}
                                                      /\ UNCHANGED completeConfig
                                           /\ UNCHANGED << curConfig, 
                                                           nextBallot >>
                                      ELSE /\ IF completeConfig.alive[req.value.p]
                                                 THEN /\ Assert(req.value.b <= curConfig.b, 
                                                                "Failure of assertion at line 470, column 23.")
                                                      /\ Assert(req.type \in {"PRIMARY_TIMEOUT","SECONDARY_TIMEOUT","PROMOTE_INACTIVE"}, 
                                                                "Failure of assertion at line 471, column 23.")
                                                      /\ curConfig' =  [req.value EXCEPT
                                                                      !.b = nextBallot,
                                                                      !.completeBal = completeConfig.b ]
                                                      /\ channel' =     (channel \ {req})
                                                                    \cup PrepareMsg(0, {curConfig'.p}, "newBallot", curConfig')
                                                      /\ nextBallot' = nextBallot + 1
                                                 ELSE /\ channel' = (channel \ {req})
                                                      /\ UNCHANGED << curConfig, 
                                                                      nextBallot >>
                                           /\ UNCHANGED completeConfig
                        /\ pc' = [pc EXCEPT ![0] = "l_manager"]
                   ELSE /\ pc' = [pc EXCEPT ![0] = "Done"]
                        /\ UNCHANGED << channel, curConfig, completeConfig, 
                                        nextBallot >>
             /\ UNCHANGED << newItem, clientNotified, clock, timer, 
                             frozenReplica, rcvTime, remoteDecree, membership, 
                             pendingPrimary, store, committed, 
                             pendingConsolidatePoint, count >>

Manager == l_manager

l == /\ pc[(TotalReplicas + 1)] = "l"
     /\ IF count < 10
           THEN /\ \/ /\ frozenReplica = 0
                      /\ \E i \in Replicas:
                           frozenReplica' = i
                      /\ UNCHANGED <<channel, count>>
                   \/ /\ frozenReplica \in Replicas
                      /\ frozenReplica' = 0
                      /\ UNCHANGED <<channel, count>>
                   \/ /\ channel /= {}
                      /\ \E ms \in channel:
                           channel' = channel \ {ms}
                      /\ count' = count +1
                      /\ UNCHANGED frozenReplica
                /\ pc' = [pc EXCEPT ![(TotalReplicas + 1)] = "l"]
           ELSE /\ frozenReplica' = 0
                /\ pc' = [pc EXCEPT ![(TotalReplicas + 1)] = "Done"]
                /\ UNCHANGED << channel, count >>
     /\ UNCHANGED << newItem, clientNotified, clock, timer, rcvTime, 
                     remoteDecree, membership, pendingPrimary, store, 
                     committed, pendingConsolidatePoint, curConfig, 
                     completeConfig, nextBallot >>

Choas == l

l_tick == /\ pc[(TotalReplicas + 2)] = "l_tick"
          /\ \E d \in 1..Lease+Grace:
               /\ /\ (\A n \in Replicas :
                      timer[n] + 1 >= d )
                  /\ (\A ms \in channel: ms.timer >= d)
               /\ clock' = clock + d
               /\ timer' = [n \in Replicas |-> timer[n] - d]
               /\ channel' = (LET F(ms) == [ms EXCEPT !.timer = ms.timer - d]
                              IN {F(x) : x \in channel })
          /\ pc' = [pc EXCEPT ![(TotalReplicas + 2)] = "l_tick"]
          /\ UNCHANGED << newItem, clientNotified, frozenReplica, rcvTime, 
                          remoteDecree, membership, pendingPrimary, store, 
                          committed, pendingConsolidatePoint, curConfig, 
                          completeConfig, nextBallot, count >>

Tick == l_tick

Next == Manager \/ Choas \/ Tick
           \/ (\E self \in Replicas: ReplicaProc(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION


\* we want to make sure there is no conflict in storage.
NoConflict == ~(\E one \in Replicas : \E two \in Replicas \ {one} :  
       /\ membership[one].alive[one] /\ membership[two].alive[two] 
       /\ committed[one] > 0 /\ committed[one] \in DOMAIN store[two]
       /\ committed[one] \in DOMAIN store[one]
       /\ store[one][committed[one]][2] # store[two][committed[one]][2]
       )

NoConflictingPrepare ==
    ~(
       \E mone \in channel : \E mtwo \in channel :
           mone.src /= mtwo.src /\ mone.type = "PREPARE" /\ mtwo.type = "PREPARE"
           /\ mone.value.membership.b = mtwo.value.membership.b
    )

\* committed should always be no larger than prepared
PrepareBeforeCommit ==
    ~(
      \E one \in Replicas : \E two \in Replicas \ {one} : 
          membership[one] = membership[two] /\ ~pendingPrimary[membership[one].p]
          /\ membership[one].alive[one] /\ membership[membership[one].p].alive[one] 
          /\ committed[two] > SeqMax(store[one])
    )

\* for a secondary in a stable membership (pendingPrimary is false), it should always has decrees bigger than committed
SecondaryInvariant ==
IF \E m \in channel : m.type = "PREPARE" /\ membership[m.dst].b <= m.value.membership.b /\ ~pendingPrimary[m.src] /\ m.value.membership.alive[m.dst] /\ membership[m.src].alive[m.dst]
THEN \E m \in channel : m.type = "PREPARE" /\ membership[m.dst].b <= m.value.membership.b /\ ~pendingPrimary[m.src] /\ m.value.membership.alive[m.dst] /\ membership[m.src].alive[m.dst]
               /\ Max(committed[m.dst], m.value.commit) <= SeqMax(store[m.dst])
ELSE TRUE


PrimaryTimeoutState ==
    /\  remoteDecree = <<<<0, 0, 0>>, <<0, 0, 0>>, <<0, 0, 0>>>>
    /\  store = <<(0 :> <<0, 0>> @@ 1 :> <<1, 21>>), (0 :> <<0, 0>>), (0 :> <<0, 0>>)>>
    /\  pendingConsolidatePoint = <<0, 0, 0>>
    /\  clock = 7
    /\  pc = ( 0 :> "l_manager" @@
      1 :> "l_replica" @@
      2 :> "l_replica" @@
      3 :> "l_replica" @@
      4 :> "l" @@
      5 :> "l_tick" )
    /\  completeConfig = [alive |-> <<TRUE, TRUE, TRUE>>, p |-> 1, b |-> 1, completeBal |-> 1]
    /\  channel = { [ type |-> "PREPARE",
        src |-> 1,
        dst |-> 2,
        value |->
            [ membership |->
                  [ alive |-> <<TRUE, TRUE, TRUE>>,
                    p |-> 1,
                    b |-> 1,
                    completeBal |-> 0 ],
              value |-> 21,
              decree |-> 1,
              ballot |-> 1,
              commit |-> 0,
              time |-> 4 ],
        timer |-> 0 ],
      [ type |-> "PREPARE",
        src |-> 1,
        dst |-> 3,
        value |->
            [ membership |->
                  [ alive |-> <<TRUE, TRUE, TRUE>>,
                    p |-> 1,
                    b |-> 1,
                    completeBal |-> 0 ],
              value |-> 21,
              decree |-> 1,
              ballot |-> 1,
              commit |-> 0,
              time |-> 4 ],
        timer |-> 0 ],
      [ type |-> "PRIMARY_TIMEOUT",
        src |-> 2,
        dst |-> 0,
        value |->
            [alive |-> <<FALSE, TRUE, TRUE>>, p |-> 2, b |-> 1, completeBal |-> 0],
        timer |-> 3 ] }
    /\  count = 0
    /\  membership = << [alive |-> <<TRUE, TRUE, TRUE>>, p |-> 1, b |-> 1, completeBal |-> 0],
       [alive |-> <<TRUE, TRUE, TRUE>>, p |-> 1, b |-> 1, completeBal |-> 0],
       [alive |-> <<TRUE, TRUE, TRUE>>, p |-> 1, b |-> 1, completeBal |-> 0] >>
    /\  rcvTime = <<<<0, 0, 0>>, <<0, 0, 0>>, <<0, 0, 0>>>>
    /\  newItem = 2
    /\  committed = <<0, 0, 0>>
    /\  frozenReplica = 1
    /\  pendingPrimary = <<FALSE, FALSE, FALSE>>
    /\  clientNotified = 0
    /\  nextBallot = 2
    /\  timer = <<-1, 2, -1>>
    /\  curConfig = [alive |-> <<TRUE, TRUE, TRUE>>, p |-> 1, b |-> 1, completeBal |-> 1]


SecondaryTakeOverState == 
    /\  remoteDecree = <<<<0, 0, 0>>, <<0, 0, 0>>, <<0, 0, 0>>>>
    /\  store = <<(0 :> <<0, 0>>), (0 :> <<0, 0>>), (0 :> <<0, 0>>)>>
    /\  pendingConsolidatePoint = <<0, 0, 0>>
    /\  clock = 7
    /\  pc = ( 0 :> "l_manager" @@
      1 :> "l_replica" @@
      2 :> "l_replica" @@
      3 :> "l_replica" @@
      4 :> "l" @@
      5 :> "l_tick" )
    /\  completeConfig = [alive |-> <<TRUE, TRUE, TRUE>>, p |-> 1, b |-> 1, completeBal |-> 1]
    /\  channel = { [ src |-> 1,
        dst |-> 2,
        type |-> "PREPARE",
        value |->
            [ membership |->
                  [ alive |-> <<TRUE, TRUE, TRUE>>,
                    p |-> 1,
                    b |-> 1,
                    completeBal |-> 0 ],
              value |-> 21,
              decree |-> 1,
              ballot |-> 1,
              commit |-> 0,
              time |-> 4 ],
        timer |-> 0 ],
      [ src |-> 1,
        dst |-> 2,
        type |-> "ACK",
        value |-> [decree |-> 0, time |-> 7],
        timer |-> 3 ],
      [ src |-> 3,
        dst |-> 2,
        type |-> "ACK",
        value |-> [decree |-> 0, time |-> 7],
        timer |-> 3 ] }
    /\  count = 2
    /\  membership = << [alive |-> <<FALSE, TRUE, TRUE>>, p |-> 2, b |-> 2, completeBal |-> 1],
       [alive |-> <<FALSE, TRUE, TRUE>>, p |-> 2, b |-> 2, completeBal |-> 1],
       [alive |-> <<FALSE, TRUE, TRUE>>, p |-> 2, b |-> 2, completeBal |-> 1] >>
    /\  rcvTime = <<<<0, 7, 0>>, <<0, 0, 0>>, <<0, 7, 0>>>>
    /\  newItem = 2
    /\  committed = <<0, 0, 0>>
    /\  frozenReplica = 3
    /\  pendingPrimary = <<FALSE, TRUE, FALSE>>
    /\  clientNotified = 0
    /\  nextBallot = 3
    /\  timer = <<-1, 2, -1>>
    /\  curConfig = [alive |-> <<FALSE, TRUE, TRUE>>, p |-> 2, b |-> 2, completeBal |-> 1]

TwoCommittedState ==  
    /\  remoteDecree = <<<<0, 0, 0>>, <<0, 0, 0>>, <<0, 3, 0>>>>
    /\  store = << (0 :> <<0, 0>>),
       (0 :> <<0, 0>> @@ 1 :> <<3, 33>> @@ 2 :> <<3, 43>> @@ 3 :> <<3, 53>>),
       (0 :> <<0, 0>> @@ 1 :> <<3, 33>> @@ 2 :> <<3, 43>> @@ 3 :> <<3, 53>>) >>
    /\  pendingConsolidatePoint = <<0, 0, 0>>
    /\  clock = 7
    /\  pc = ( 0 :> "l_manager" @@
      1 :> "l_replica" @@
      2 :> "l_replica" @@
      3 :> "l_replica" @@
      4 :> "l" @@
      5 :> "l_tick" )
    /\  completeConfig = [alive |-> <<FALSE, TRUE, TRUE>>, b |-> 3, p |-> 3, completeBal |-> 2]
    /\  channel = { [ src |-> 2,
        dst |-> 3,
        type |-> "ACK",
        value |-> [decree |-> 2, time |-> 7],
        timer |-> 3 ],
      [ src |-> 3,
        dst |-> 2,
        type |-> "PREPARE",
        value |->
            [ membership |->
                  [ alive |-> <<FALSE, TRUE, TRUE>>,
                    b |-> 3,
                    p |-> 3,
                    completeBal |-> 2 ],
              value |-> 0,
              decree |-> 0,
              ballot |-> 0,
              commit |-> 0,
              time |-> 7 ],
        timer |-> 3 ] }
    /\  count = 5
    /\  membership = << [alive |-> <<FALSE, TRUE, TRUE>>, b |-> 2, p |-> 2, completeBal |-> 1],
       [alive |-> <<FALSE, TRUE, TRUE>>, b |-> 3, p |-> 3, completeBal |-> 2],
       [alive |-> <<FALSE, TRUE, TRUE>>, b |-> 3, p |-> 3, completeBal |-> 2] >>
    /\  rcvTime = <<<<0, 7, 0>>, <<0, 0, 7>>, <<0, 7, 0>>>>
    /\  newItem = 5
    /\  committed = <<0, 1, 3>>
    /\  frozenReplica = 3
    /\  pendingPrimary = <<FALSE, TRUE, FALSE>>
    /\  clientNotified = 3
    /\  nextBallot = 4
    /\  timer = <<-1, 2, 2>>
    /\  curConfig = [alive |-> <<FALSE, TRUE, TRUE>>, b |-> 3, p |-> 3, completeBal |-> 2]


SecondaryTimeoutState == 
    /\  remoteDecree = <<<<0, 0, 0>>, <<0, 0, 0>>, <<0, 0, 0>>>>
    /\  store = <<(0 :> <<0, 0>>), (0 :> <<0, 0>>), (0 :> <<0, 0>>)>>
    /\  pendingConsolidatePoint = <<0, 0, 0>>
    /\  clock = 5
    /\  pc = ( 0 :> "l_manager" @@
      1 :> "l_replica" @@
      2 :> "l_replica" @@
      3 :> "l_replica" @@
      4 :> "l" @@
      5 :> "l_tick" )
    /\  completeConfig = [alive |-> <<TRUE, TRUE, TRUE>>, b |-> 1, p |-> 1, completeBal |-> 1]
    /\  channel = { [ src |-> 0,
        dst |-> 1,
        type |-> "newBallot",
        value |->
            [alive |-> <<TRUE, FALSE, TRUE>>, b |-> 3, p |-> 1, completeBal |-> 1],
        timer |-> 3 ],
      [ src |-> 1,
        dst |-> 3,
        type |-> "ACK",
        value |-> [decree |-> 0, time |-> 5],
        timer |-> 3 ],
      [ src |-> 3,
        dst |-> 0,
        type |-> "complete",
        value |->
            [alive |-> <<TRUE, FALSE, TRUE>>, b |-> 4, p |-> 3, completeBal |-> 1],
        timer |-> 3 ],
      [ src |-> 3,
        dst |-> 2,
        type |-> "PREPARE",
        value |->
            [ membership |->
                  [ alive |-> <<TRUE, FALSE, TRUE>>,
                    b |-> 4,
                    p |-> 3,
                    completeBal |-> 1 ],
              value |-> 0,
              decree |-> 0,
              ballot |-> 0,
              commit |-> 0,
              time |-> 5 ],
        timer |-> 3 ] }
    /\  count = 1
    /\  membership = << [alive |-> <<TRUE, FALSE, TRUE>>, b |-> 4, p |-> 3, completeBal |-> 1],
       [alive |-> <<TRUE, TRUE, TRUE>>, b |-> 1, p |-> 1, completeBal |-> 0],
       [alive |-> <<TRUE, FALSE, TRUE>>, b |-> 4, p |-> 3, completeBal |-> 1] >>
    /\  rcvTime = <<<<0, 0, 5>>, <<0, 0, 0>>, <<0, 0, 0>>>>
    /\  newItem = 1
    /\  committed = <<0, 0, 0>>
    /\  frozenReplica = 1
    /\  pendingPrimary = <<FALSE, FALSE, TRUE>>
    /\  clientNotified = 0
    /\  nextBallot = 5
    /\  timer = <<2, 1, 2>>
    /\  curConfig = [alive |-> <<TRUE, FALSE, TRUE>>, b |-> 4, p |-> 3, completeBal |-> 1]


CompetePrimaryState ==
        /\  remoteDecree = <<<<0, 0, 0>>, <<0, 0, 0>>, <<0, 0, 0>>>>
        /\  store = <<(0 :> <<0, 0>>), (0 :> <<0, 0>>), (0 :> <<0, 0>>)>>
        /\  pendingConsolidatePoint = <<0, 0, 0>>
        /\  clock = 7
        /\  pc = ( 0 :> "l_manager" @@
          1 :> "l_replica" @@
          2 :> "l_replica" @@
          3 :> "l_replica" @@
          4 :> "l" @@
          5 :> "l_tick" )
        /\  completeConfig = [p |-> 2, b |-> 3, alive |-> <<FALSE, TRUE, TRUE>>, completeBal |-> 1]
        /\  channel = { [ src |-> 0,
            dst |-> 2,
            type |-> "newBallot",
            value |->
                [p |-> 2, b |-> 5, alive |-> <<TRUE, TRUE, TRUE>>, completeBal |-> 3],
            timer |-> 3 ],
          [ src |-> 0,
            dst |-> 3,
            type |-> "newBallot",
            value |->
                [p |-> 3, b |-> 4, alive |-> <<FALSE, TRUE, TRUE>>, completeBal |-> 3],
            timer |-> 3 ],
          [ src |-> 1,
            dst |-> 3,
            type |-> "PREPARE",
            value |->
                [ membership |->
                      [ p |-> 1,
                        b |-> 1,
                        alive |-> <<TRUE, TRUE, TRUE>>,
                        completeBal |-> 0 ],
                  value |-> 1,
                  decree |-> 1,
                  ballot |-> 1,
                  commit |-> 0,
                  time |-> 4 ],
            timer |-> 0 ],
          [ src |-> 3,
            dst |-> 2,
            type |-> "ACK",
            value |-> [decree |-> 0, time |-> 7],
            timer |-> 3 ] }
        /\  count = 5
        /\  membership = << [p |-> 2, b |-> 3, alive |-> <<FALSE, TRUE, TRUE>>, completeBal |-> 1],
           [p |-> 2, b |-> 3, alive |-> <<TRUE, TRUE, TRUE>>, completeBal |-> 1],
           [p |-> 2, b |-> 3, alive |-> <<FALSE, TRUE, TRUE>>, completeBal |-> 1] >>
        /\  rcvTime = <<<<0, 7, 0>>, <<0, 0, 0>>, <<0, 7, 0>>>>
        /\  newItem = 1
        /\  committed = <<0, 0, 0>>
        /\  frozenReplica = 1
        /\  pendingPrimary = <<FALSE, FALSE, FALSE>>
        /\  clientNotified = 0
        /\  nextBallot = 6
        /\  timer = <<-1, 2, -1>>
        /\  curConfig = [p |-> 2, b |-> 5, alive |-> <<TRUE, TRUE, TRUE>>, completeBal |-> 3]


\* This state happens when a dead machine is promoted to secondary, but before
\* that promtion message got to master, primary is changed, and new items are
\* committed without considering the "dead" machine's vote. Now the promotion
\* message reached master. So we have a new "secondary" with a prepare queue
\* that lags behind. Let's see whether it would cause any trouble.
AbruptInactivePromotionState ==
        /\  remoteDecree = <<<<0, 0, 0>>, <<0, 0, 0>>, <<0, 1, 0>>>>
        /\  store = << (0 :> <<0, 0>>),
           (0 :> <<0, 0>> @@ 1 :> <<4, 3>> @@ 2 :> <<5, 3>>),
           (0 :> <<0, 0>> @@ 1 :> <<4, 3>> @@ 2 :> <<4, 3>>) >>
        /\  pendingConsolidatePoint = <<0, 2, 0>>
        /\  clock = 7
        /\  pc = ( 0 :> "l_manager" @@
          1 :> "l_replica" @@
          2 :> "l_replica" @@
          3 :> "l_replica" @@
          4 :> "l" @@
          5 :> "l_tick" )
        /\  completeConfig = [b |-> 4, p |-> 3, alive |-> <<FALSE, TRUE, TRUE>>, completeBal |-> 3]
        /\  channel = { [ src |-> 2,
            dst |-> 1,
            type |-> "PREPARE",
            value |->
                [ value |-> 3,
                  membership |->
                      [ b |-> 5,
                        p |-> 2,
                        alive |-> <<TRUE, TRUE, TRUE>>,
                        completeBal |-> 3 ],
                  decree |-> 2,
                  ballot |-> 5,
                  commit |-> 1,
                  time |-> 7 ],
            timer |-> 3 ],
          [ src |-> 2,
            dst |-> 3,
            type |-> "PREPARE",
            value |->
                [ value |-> 3,
                  membership |->
                      [ b |-> 5,
                        p |-> 2,
                        alive |-> <<TRUE, TRUE, TRUE>>,
                        completeBal |-> 3 ],
                  decree |-> 2,
                  ballot |-> 5,
                  commit |-> 1,
                  time |-> 7 ],
            timer |-> 3 ],
          [ src |-> 2,
            dst |-> 3,
            type |-> "ACK",
            value |-> [decree |-> 2, time |-> 7],
            timer |-> 3 ],
          [ src |-> 3,
            dst |-> 2,
            type |-> "PREPARE",
            value |->
                [ value |-> 0,
                  membership |->
                      [ b |-> 4,
                        p |-> 3,
                        alive |-> <<FALSE, TRUE, TRUE>>,
                        completeBal |-> 3 ],
                  decree |-> 0,
                  ballot |-> 0,
                  commit |-> 0,
                  time |-> 7 ],
            timer |-> 3 ] }
        /\  count = 7
        /\  membership = << [b |-> 3, p |-> 2, alive |-> <<FALSE, TRUE, TRUE>>, completeBal |-> 1],
           [b |-> 5, p |-> 2, alive |-> <<TRUE, TRUE, TRUE>>, completeBal |-> 3],
           [b |-> 4, p |-> 3, alive |-> <<FALSE, TRUE, TRUE>>, completeBal |-> 3] >>
        /\  rcvTime = <<<<0, 7, 0>>, <<0, 0, 7>>, <<0, 7, 0>>>>
        /\  newItem = 2
        /\  committed = <<0, 1, 1>>
        /\  frozenReplica = 1
        /\  pendingPrimary = <<FALSE, TRUE, FALSE>>
        /\  clientNotified = 1
        /\  nextBallot = 6
        /\  timer = <<-1, 2, 2>>
        /\  curConfig = [b |-> 5, p |-> 2, alive |-> <<TRUE, TRUE, TRUE>>, completeBal |-> 4]
    
=============================================================================
\* Modification History
\* Last modified Mon Jun 22 17:17:13 PDT 2015 by chenfu
\* Created Mon Jun 01 09:21:53 PDT 2015 by chenfu



