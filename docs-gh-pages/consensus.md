## Consensus

### Architecture
Every new `CheckpointBlock` is being created under a consensus round.

### Glossary
- **consensus owner** - the node which sent consensus proposal to others and initiated a new round
- **facilitator** - node which has been selected to take part in a new round and responded successfully to participate in it (including owner)
- **SOE** - signed observation edge (see: )
- **data proposal** - transactions (and other checkpoint block data) proposed by the facilitator to include in the new consensus round

### Consensus round
1. Start consensus
2. Broadcast and gather consensus data proposals
3. Merge, broadcast and gather consensus block proposals
4. Select a majority block proposal and merge facilitators' signatures
5. Accept a finished block and broadcast it to non-facilitators

### Stages

#### 1. Start consensus
`ConsensusScheduler` runs `consensusManager.startOwnConsensus()` every **n** seconds
(*default value: 10s*, configurable under `constellation.consensus.start-own-interval`).
To start a new round, node must be in one of valid node states. Otherwise, creating new round will be skipped.
Valid node states for starting new consensus round are `Ready` and `SnapshotCreation`.

When new round can be created, owner creates one by using pending transactions (up to `maxTransactionThreshold`), tip SOE
and selects final facilitators. Peers which can take part in the consensus round must have `nextSnapshotHeight` below
the proposed tip. Otherwise, the selected node will not be able to accept the final checkpoint block. Facilitators are being
selected randomly taking into account above preconditions.

**Dummy transaction** is a transaction pulled to the consensus round if node has no real transactions. Node should always
be able to create a block, even if the block is empty. To distinguish empty blocks, there must be at least one dummy transaction
which is equivalent to the real transaction but carries 0 amount.

#### 2. Broadcast and gather consensus data proposals
Owner sends the `NotifyFacilitators` message to all selected facilitators and waits for the response. If facilitator is able
to start a consensus, responds with success message and creates a corresponding round. Facilitator cannot start a consensus
if proposed tip is below the next snapshot height or is in the incorrect state (i.e. leaving from the cluster).
When all facilitators have been notified and started rounds successfully, each one pulls transactions, similarly as owner did,
and broadcasts it to all consensus round's participants. In parallel, each participant gathers proposals from others and keeps it locally.

#### 3. Merge, broadcast and gather consensus block proposals
When node receives all proposals, it merges them together to create a block proposal and then broadcasts it to others.
Such a block proposal is being signed by the proposer only. The same process happens on every node taking part in a consensus round,
hence when data used for block proposal creation are the same, block proposals will be equal (except signatures which obviously must be different). 

#### 4. Select a majority block proposal and merge facilitator's signatures
When node receives all block proposals, it verifies the correctness of proposals. The majority block is being chosen
and all edges from corresponding blocks are merged together creating one block with all signatures. Such a block
is broadcasted to other facilitators as **selected** one.

#### 5. Accept a finished block and broadcast it to non-facilitators
When node receives confirmation from other participants that the selected block is the same across all facilitators,
it starts acceptance flow for the selected block. If block has been accepted successfully, node broadcasts it to all non-facilitators.
It simply means that every other node in the network receives the newly created block and tries to accept it.

### Clean up
Every own and participating consensus round has a total timeout specified by `form-checkpoint-blocks-timeout` configuration entry.
If the round takes longer or basically stopped at some point, the cleaning process will close such round. Transactions
and observations proposed in the round will be taken back to the pool and proposed in another round. The same applies if consensus
round has been finished with an error.

### Errors
- `OwnRoundAlreadyInProgress` - cannot start a new round because another own one is in progress
- `NoTipsForConsensus` - there are no tips to start a new round
- `NoPeersForConsensus` - there are no peers to start a new round with
- `NotAllPeersParticipate` - one of selected facilitator rejected participating in the round
(i.e. when proposed tip is above the latest created snapshot height) 
- `NotEnoughProposals` - if node receives fewer proposals than expected

### Configuration
```hocon
consensus {
    maxTransactionThreshold = 50
    maxObservationThreshold = 50
    start-own-interval = 5s
    cleanup-interval = 10s
    union-proposals-timeout = 10s
    checkpoint-block-resolve-majority-timeout = 10s
    form-checkpoint-blocks-timeout = 40s
  }
```

- `maxTransactionThreshold` (_default_: **50**) - maximum transactions that can be pulled to one round
- `maxObservationThreshold` (_default_: **50**) - maximum observations that can be pulled to one round
- `start-own-interval` (_default_: **5s**) - how often trigger creating new round
- `cleanup-interval` (_default_: **10s**) - how often trigger cleaning up flow
- `union-proposals-timeout` (_default_: **10s**) - timeout for gathering data proposals
- `checkpoint-block-resolve-majority-timeout` (_default_: **10s**) - timeout for gathering block proposals
- `form-checkpoint-blocks-timeout` (_default_: **40s**) - timeout for the whole consensus round