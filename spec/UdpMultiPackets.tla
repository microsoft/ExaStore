-------------------------- MODULE UdpMultiPackets --------------------------

\* This is a multi-packet transportation protocol over UDP
\* A Sender has PckCount number of packets to send to an Receiver, over a
\* unreliable UDP network, which could lose or reorder packets.
\*
\* The sender sends two packets a time, wait for ack before sending the next
\* two packets. If sender timeout before receiving expected ack, it resends
\* the two packets.
\* 
\* The receiver waits for packets to arrive in-sequence. All out of sequence
\* packets are discarded. Ack packets are sent to the receiver for every two
\* packets received, indicating that the sender can send more packets.
\*
\* We want to ensures that under strong fairness (if infinitely often m is
\* in the message queue then it is eventually received), all packets are 
\* eventually received.
\*
\* Thanks Dr. Lamport for guidance! 

EXTENDS Naturals, Sequences
CONSTANT PckCount
ASSUME PckCount \in Nat \ {0}

Remove(i, seq) == [ j \in 1..(Len(seq) - 1) |-> IF j < i THEN seq[j] ELSE seq[j+1]]

(******************************************************************************
--algorithm ExaUdpProtocol 
{
  \* msgC channel Sender -> Receiver
  \* ackC channel Receiver -> Receiver
  \* output contains received packets
  \* each packet is represented by its sequence number, i.e. an integer
  
  variables output = <<>>; msgC = <<>>; ackC = <<>>;
  
  macro Send(m, chan) { 
      chan := Append(chan, m) 
  }
  
  \* Receive can get packet in the middle of the channel
  \* to simulate packet arriving out of order
  macro Rcv(v, chan) {
      await chan /= <<>>;
      with ( i \in 1..Len(chan))
      {
          v := chan[i];
          chan := Remove(i, chan);
      } 
  }
  
  fair process (Sender = "S")
      variables toSend = 0; ack = 0;
  {
    l_send:
      while (toSend < PckCount) {
          either {
              Send(toSend, msgC);
            l_send_second:         
              if (toSend > 0 /\ (toSend+1) < PckCount)
              {
                  Send(toSend+1, msgC);
              }
          }
          or {
              Rcv(ack, ackC);
            l_depart:
              \* if ack received, send next two packets
              if (ack = 1 \/ ack = (toSend + 2) \/ ack = PckCount) {
                  toSend := ack;
              }
          } 
      }
  }
  
  fair process (Receiver = "R")
      variables next = 0; msg = 0; 
  {
    l_rcv:
      while (next < PckCount) {
          Rcv(msg, msgC);
        l_accept:
          if (msg = next) {
              next := next + 1 ;
              output := Append(output, msg)
          };
        l_ack:
          if ( (next % 2) = 1)
          {
              Send(next, ackC);
          }
      };
      
    l_finish_rcv:
      Send(next, ackC);
  }
  
  process (LoseMsg = "L")
      variables count = 0;
  {
      \* Simulate loss of packet over network
      l: while (count < PckCount) {
          either with ( i \in 1..Len(msgC)) { msgC := Remove(i, msgC) }
          or     with ( i \in 1..Len(ackC)) { ackC := Remove(i, ackC) };
          count := count +1;
      }
  }

}
******************************************************************************)
\* BEGIN TRANSLATION
VARIABLES output, msgC, ackC, pc, toSend, ack, next, msg, count

vars == << output, msgC, ackC, pc, toSend, ack, next, msg, count >>

ProcSet == {"S"} \cup {"R"} \cup {"L"}

Init == (* Global variables *)
        /\ output = <<>>
        /\ msgC = <<>>
        /\ ackC = <<>>
        (* Process Sender *)
        /\ toSend = 0
        /\ ack = 0
        (* Process Receiver *)
        /\ next = 0
        /\ msg = 0
        (* Process LoseMsg *)
        /\ count = 0
        /\ pc = [self \in ProcSet |-> CASE self = "S" -> "l_send"
                                        [] self = "R" -> "l_rcv"
                                        [] self = "L" -> "l"]

l_send == /\ pc["S"] = "l_send"
          /\ IF toSend < PckCount
                THEN /\ \/ /\ msgC' = Append(msgC, toSend)
                           /\ pc' = [pc EXCEPT !["S"] = "l_send_second"]
                           /\ UNCHANGED <<ackC, ack>>
                        \/ /\ ackC /= <<>>
                           /\ \E i \in 1..Len(ackC):
                                /\ ack' = ackC[i]
                                /\ ackC' = Remove(i, ackC)
                           /\ pc' = [pc EXCEPT !["S"] = "l_depart"]
                           /\ msgC' = msgC
                ELSE /\ pc' = [pc EXCEPT !["S"] = "Done"]
                     /\ UNCHANGED << msgC, ackC, ack >>
          /\ UNCHANGED << output, toSend, next, msg, count >>

l_send_second == /\ pc["S"] = "l_send_second"
                 /\ IF toSend > 0 /\ (toSend+1) < PckCount
                       THEN /\ msgC' = Append(msgC, (toSend+1))
                       ELSE /\ TRUE
                            /\ msgC' = msgC
                 /\ pc' = [pc EXCEPT !["S"] = "l_send"]
                 /\ UNCHANGED << output, ackC, toSend, ack, next, msg, count >>

l_depart == /\ pc["S"] = "l_depart"
            /\ IF ack = 1 \/ ack = (toSend + 2) \/ ack = PckCount
                  THEN /\ toSend' = ack
                  ELSE /\ TRUE
                       /\ UNCHANGED toSend
            /\ pc' = [pc EXCEPT !["S"] = "l_send"]
            /\ UNCHANGED << output, msgC, ackC, ack, next, msg, count >>

Sender == l_send \/ l_send_second \/ l_depart

l_rcv == /\ pc["R"] = "l_rcv"
         /\ IF next < PckCount
               THEN /\ msgC /= <<>>
                    /\ \E i \in 1..Len(msgC):
                         /\ msg' = msgC[i]
                         /\ msgC' = Remove(i, msgC)
                    /\ pc' = [pc EXCEPT !["R"] = "l_accept"]
               ELSE /\ pc' = [pc EXCEPT !["R"] = "l_finish_rcv"]
                    /\ UNCHANGED << msgC, msg >>
         /\ UNCHANGED << output, ackC, toSend, ack, next, count >>

l_accept == /\ pc["R"] = "l_accept"
            /\ IF msg = next
                  THEN /\ next' = next + 1
                       /\ output' = Append(output, msg)
                  ELSE /\ TRUE
                       /\ UNCHANGED << output, next >>
            /\ pc' = [pc EXCEPT !["R"] = "l_ack"]
            /\ UNCHANGED << msgC, ackC, toSend, ack, msg, count >>

l_ack == /\ pc["R"] = "l_ack"
         /\ IF (next % 2) = 1
               THEN /\ ackC' = Append(ackC, next)
               ELSE /\ TRUE
                    /\ ackC' = ackC
         /\ pc' = [pc EXCEPT !["R"] = "l_rcv"]
         /\ UNCHANGED << output, msgC, toSend, ack, next, msg, count >>

l_finish_rcv == /\ pc["R"] = "l_finish_rcv"
                /\ ackC' = Append(ackC, next)
                /\ pc' = [pc EXCEPT !["R"] = "Done"]
                /\ UNCHANGED << output, msgC, toSend, ack, next, msg, count >>

Receiver == l_rcv \/ l_accept \/ l_ack \/ l_finish_rcv

l == /\ pc["L"] = "l"
     /\ IF count < PckCount
           THEN /\ \/ /\ \E i \in 1..Len(msgC):
                           msgC' = Remove(i, msgC)
                      /\ ackC' = ackC
                   \/ /\ \E i \in 1..Len(ackC):
                           ackC' = Remove(i, ackC)
                      /\ msgC' = msgC
                /\ count' = count +1
                /\ pc' = [pc EXCEPT !["L"] = "l"]
           ELSE /\ pc' = [pc EXCEPT !["L"] = "Done"]
                /\ UNCHANGED << msgC, ackC, count >>
     /\ UNCHANGED << output, toSend, ack, next, msg >>

LoseMsg == l

Next == Sender \/ Receiver \/ LoseMsg
           \/ (* Disjunct to prevent deadlock on termination *)
              ((\A self \in ProcSet: pc[self] = "Done") /\ UNCHANGED vars)

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Sender)
        /\ WF_vars(Receiver)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

\* Safety condition; verifies that the receiver gets every packets when done
PartialCorrectness == (pc["R"] = "Done") => (Len(output) = PckCount) /\ (output = [ j \in 1..Len(output) |-> (j-1) ])

\* fairness condition, this instead of Spec is what we need to fill in "What is the behavior spec -> Temporal aormula
StrongFairness == /\ Spec
                 /\ SF_vars (Sender /\ (ackC'  /= ackC ))
                 /\ SF_vars (Sender /\ (msgC' /= msgC))
                 /\ SF_vars (Receiver /\ (msgC' /= msgC))
                 /\ SF_vars (Receiver /\ (ackC' /= ackC ))
                 
                 
\*                 /\ (\A m \in 1..(PckCount-1) : 
\*                        SF_vars( Receiver /\ ( \E i \in 1..Len(msgC) : (msgC[i] = m) /\ (msg' = m) ) ) )
\*                 /\ (\A m \in 1..(PckCount-1) : 
\*                        SF_vars( Sender /\ ( \E i \in 1..Len(ackC) : (ackC[i] = m) /\ (ack' = m) ) ) )
                 

=============================================================================
\* Modification History
\* Last modified Wed May 06 09:50:00 PDT 2015 by chenfu
\* Created Thu Apr 23 16:33:20 PDT 2015 by chenfu

