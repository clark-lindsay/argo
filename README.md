# Argo

An implementation of the [Raft distributed consensus algorithm](https://raft.github.io/) for fun and learning.

Why "Argo"? Because a raft is a sort of boat, and I can't imagine a better "raft" for escaping from the island of [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) than [the Argo](https://en.wikipedia.org/wiki/Argo).

## Disclaimer

First things first: this implementation should not be used for anything except the aforementioned goals of "fun and learning".

I wrote this because I was first intrigued by the protocol, and then enamored by actually being able to understand it after reading [the paper](https://raft.github.io/raft.pdf), which was different from my experience with [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) where I was first intrigued, and then bewildered (the ["made simple" paper](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) is still a good read, though).

## Corners that I cut

Non-exhaustive list of things that I cut out because they either didn't seem valuable given the goals of "fun and learning" or I just haven't gotten around to it:

- servers writing their log to non-volatile storage
- each server running a state machine, fed with events from the log
   - will likely be implemented with [gen_statem](https://www.erlang.org/doc/man/gen_statem)
- distribution across nodes
   - i.e. i am using a registry for cluster membership, instead of a distributed solution like [pg](https://www.erlang.org/doc/man/pg.html)

