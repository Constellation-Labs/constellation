# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
- SoftStaking nodes rewards

## [v2.28.2] 2021-09-22
### Changed
- Whitelisting updated

## [v2.28.1] 2021-09-01
### Changed
- Whitelisting updated

## [v2.28.0] 2021-07-27
### Changed
- Reduced snapshot delays

## [v2.27.0] 2021-07-19
### Changed
- Abstract away healthcheck and implement proposal healthcheck
- Update whitelisting file

## [v2.26.1] 2021-07-15
### Fixed
- Unmark checkpoint from resolving when skipping

## [v2.26.0] 2021-07-12
### Changed
- Validate checkpoint block before adding it to the acceptance queue
- Update whitelisting file

## [v2.25.2] 2021-07-09
### Fixed
- Clear resolving queue on setSnapshot

## [v2.25.1] 2021-07-07
### Changed
- Update whitelisting file

## [v2.25.0] 2021-07-07
### Fixed
- Fixed signature validation for gossip messages when payload contains more than one empty string
### Changed
- Joining height is defined separately for download and for set
- Resolving checkpoints in queue with max concurrent limit
- Improve transaction data consistency
- Update whitelisting file

## [v2.24.10] 2021-06-29
### Changed
- Do not resolve blocks on compare that are known on the node

## [v2.24.9] 2021-06-29
### Changed
- Ignore blocks to compare below last snapshot height

## [v2.24.8] 2021-06-29
### Fixed
- Exclude parents

## [v2.24.7] 2021-06-29
### Fixed
- Add missing but existing tips to acceptance

## [v2.24.6] 2021-06-29
### Added
- Compare checkpoints at heights
### Changed
- Start resolving missing references and parents in a fiber
### Fixed
- Gossip path logging

## [v2.24.5] 2021-06-25
### Changed
- Distance from majority for snapshots and consensuses

## [v2.24.4] 2021-06-24
### Changed
- Reduced max snapshot distance from majority to 6
- Tips usages and clearing stale tips

## [v2.24.3] 2021-06-24
### Fixed
- Exclude min waiting height from snapshot preconditions

## [v2.24.2] 2021-06-24
### Fixed
- Fetch initial majority at joining height
- Set transactions and observations on set snapshot method

## [v2.24.1] 2021-06-23
### Fixed
- Initial joining cutoff height
- Select fully joined peers for resolving and broadcasting
- Wallet last transaction reference

## [v2.24.0] 2021-06-23
### Added
- Gossiping for checkpoint blocks
### Changed
- Whitelisting update
- Majority selection algorithm
- Missing proposal lookup algorithm
- DAO usage removed
- Initialization flow changed
- Checkpoint statuses
- Checkpoint acceptance flow
- Split joining height and broadcasting logic
- SnapshotInfo class schema change
- Metrics
### Fixed
- Tip usages cache
- Initialize kryo for wallet


## [v2.23.4] 2021-06-22
### Changed
- Disabled addition of missing peers after health check consensus

## [v2.23.3] 2021-04-19
### Changed
- Missing proposal offset set to 0
- Rename of a duplicated alias in the whitelisting
### Reverted
- Majority selected with no gaps

## [v2.23.2] 2021-04-16
### Changed
- Gossip flow improvements
- Majority selected with no gaps

## [v2.23.1] 2021-04-14
### Changed
- Update whitelisting file

## [v2.23.0] 2021-04-13
### Added
- Gossip protocol implementation for spreading snapshot proposals

## [v2.22.3] 2021-04-12
### Changed
- Multiple small healthcheck improvements:
    - Logging and metrics for peer removal and healthcheck decision
    - Running time heavy processes concurrently not to block the main management flow
    - Check if healthcheck for an unresponsive peer wasn't run recently before running new healthcheck
    - Clear a Set of peers to check after fetching them to prevent running checks continuously for the same peers
- Java update in docker images
- Update whitelisting file

## [v2.22.2] 2021-03-17
### Changed
- Prevent running periodic redownload until download finishes

## [v2.22.1] 2021-03-07
### Changed
- Better tx destination address validation (parity number and base58 characters)
- Update whitelisting file

## [v2.22.0] 2021-02-05
### Added
- New implementation of Peer Healthcheck based on consensus between nodes

## [v2.21.3] 2021-01-25
### Added
- Endpoint with total supply

### Changed
- Update whitelisting file

## [v2.21.2] 2021-01-15
### Changed
- Update whitelisting file

## [v2.21.1] 2021-01-08
### Changed
- Remove node operator

## [v2.21.0] 2020-12-31
### Added
- Validation of transactions ordinal chain during block acceptance
- Splunk support
### Changed
- Pending transactions that no longer can be accepted are removed
- Order of block transaction acceptance (based on ordinal)
- Fail the joining flow and leave the cluster if any step during the joining process fails

## [v2.20.0] 2020-12-15
### Added
- Reference implementation of concise transaction data format
- Reference implementation of bip44
### Changed
- Remove two node operators
- Skip removing genesis tips
- Objects in mem-pool no longer expire after some time
- Removed twitter chill and algebird from wallet dependencies and moved them to schema and core
- Small rearrangement of dependencies in build file

## [v2.19.0] 2020-11-16
### Added
- Additional metrics for non-dummy transactions
- Validation that transaction chain doesn't have any gaps
### Changed
- Turn off dummy transactions
- More verbose public endpoints

## [v2.18.0] 2020-11-12
### Added
- Whitelisted next batch of operators

## [v2.17.1] 2020-11-05
### Fixed
- Nodes should no longer stuck in `PendingDownload` when joining the cluster

## [v2.17.0] 2020-11-03
### Added
- Possibility to migrate existing KeyStore to use storepass for both KeyStore and KeyPair
- Possibility to export private key from KeyStore in hexadecimal format
- Endpoint for querying transactions by source address, that are not yet in a snapshot
### Changed
- ConstellationNode uses fs2 implementation to initialize
- Node downloads only meaningful snapshots when joining the cluster
- Removed one node operators

## [v2.16.2] 2020-10-09
### Changed
- Remove two node operators from whitelisting
### Fixed
- Mark snapshot and snapshot-info files as sent for nodes not sending to cloud in order to not accumulate files locally

## [v2.16.1] 2020-10-01
### Changed
- Disabled size limit for snapshot directory

## [v2.16.0] 2020-09-30
### Changed
- At least 4 CPU cores are required to start a node
### Fixed
- Bounded thread pool usage for CPU-heavy operations

## [v2.15.1] 2020-09-29
### Fixed
- ConstellationNode init (execution context)

## [v2.15.0] 2020-09-25
### Breaking changes
- New service port: 9003

### Added
- Fallback to other buckets while doing rollback

### Changed
- Removed libraries: scala-async, akka, storehaus-cache, sway, slick, h2, hasher, grgen
- Updated libraries: http-request-signer, enumeratum-circe, scaffeine, ext-scalatags, better-files, pureconfig, log4cats-slf4j, logstash-logback-encoder, http4s, prometheus, cats, aws
- Updated scala test versions
- Run API calls on unbounded thread pool
- Run http4s server on callbacks thread pool
- Run peer health check on a separate server with a separate thread pool
- Extracted schema out of the root project
- Extracted kryo registration out of the root project
- Changed from ioapp to unbounded thread pool

## [v2.14.3] 2020-09-11
### Changed
- Further adjusted rollback process

## [v2.14.2] 2020-09-11
### Fixed
- Lock existing memPool during redownload
- Adjust rollback process

## [v2.14.1] 2020-09-11
### Changed
- Use more descriptive names for methods which clear the mempool during snapshot creation/redownload

### Fixed
- Use lock when updating balances in AddressService during redownload/rollback
- Apply SnapshotInfo balances all at once instead of one by one during redownload
- Make sure that we clear balances which existed at higher heights in AddressService when redownload moves node back to lower heights
- Use Set for locking addresses everywhere for consistency and to avoid accidental double-lock
- Improvements for rollback process

## [v2.14.0] 2020-08-26
### Added
- Refuse starting node if there is not enough space left on device
- Auto leave if there is no space left on device for snapshot

## [v2.13.10] 2020-08-22
### Fixed
- Handle corner cases with gap finding

## [v2.13.9] 2020-08-20
### Added
- Additional logs for potential gaps in majority state
### Fixed
- Revert health checks during PendingDownload state

## [v2.13.8] 2020-08-19
### Fixed
- Gaps in cloud queue fixed by calculating diff between majority and already sent

## [v2.13.7] 2020-08-12
### Changed
- Changed whitelisting file

## [v2.13.6] 2020-08-11
### Fixed
- Sorting of queued snapshots for cloud backup

## [v2.13.5] 2020-08-06
### Changed
- Revert changes in KeyStoreUtils and the change from SpongyCastle to BouncyCastle - caused errors on linux env
- Update whitelisting

## [v2.13.4] 2020-08-05
### Changed
- Changed ID of one of node operators

## [v2.13.3] 2020-08-05
### Changed
- Use aliases instead of ips in Grafana
- Adjusted KeyStoreUtils for CoMakery integration
- Removed SpongyCastle and replaced it with BouncyCastle
- Whitelisted all batch1 operators
### Fixed
- Adjust reputation to observations

## [v2.13.2] 2020-07-31
### Fixed
- Adjusted PeerHealthCheck node statuses 

## [v2.13.1] 2020-07-31
### Added
- Alias available in node metrics
### Changed
- Adjusted PeerHealthCheck 

## [v2.13.0] 2020-07-29
### Added
- Possibility to set alias for node in whitelisting file (and including it in cluster/info)
### Changed
- Remove IP from whitelisting and introduce dynamic IP to Id mapping
- Remove offline peers after snapshot
- Whitelisted new operators

## [v2.12.0] 2020-07-17
### Changed
- New operators whitelisted

## [v2.12.0-rc1] 2020-07-16
### Changed
- Adjustment for SelfAvoidingWalk

## [v2.11.3] - 2020-07-09
### Changed
- Change IP of node operator

## [v2.11.2] - 2020-07-03
### Fixed
- Calculate stale tips using max of next snapshot heights

## [v2.11.1] - 2020-07-03
### Fixed
- Mark snapshot as sent only if sent to at least one provider
- Do not send and reward unaccepted majority snapshots
- Calculate stale tips by the occurrences

## [v2.11.0] - 2020-07-02
### Changed
- Revert: remove stale tips at higher height
- Change `snapshotPerMinute` from `4` to `2` as rewards adjustment

## [v2.10.0] - 2020-06-30
### Added
- Whitelist new operators
### Changed
- Remove stale tips at higher height

## [v2.10.0-rc1] - 2020-06-29
### Added
- Support for cloud providers and queue sending
- Additional logs for batch-fetching by merkle root
- Session token for communication between peers #1284
### Fixed
- Remove snapshot only if snapshot was send to cloud
- Distinct trust nodes for SelfAvoidingWalk

## [v2.9.1] - 2020-06-17 
### Fixed
- Fix union checkpoint block creation during consensus
- Fix stack overflow in SelfAvoidingWalk
- Fix broadcasting finished checkpoint block

## [v2.9.1-rc1] - 2020-06-12
### Fixed
- Fixed missing parents in consensus
- Fixed circe mappings for ADTs

## [v2.9.0] - 2020-06-11
### Changed
- Added @tailrec for SelfAvoidingWalk
- Changed scheduler delay for stale checks

## [v2.9.0-rc2] - 2020-06-10
### Fixed
- Check blocks below the snapshot height before missing parents 

## [v2.9.0-rc1] - 2020-06-10
### Added
- Dead peer caching #1239
### Changed
- Circe update to v0.13.0
- Get rid of circe auto derivation
- Data resolver refactor
- Different reputation does not change snapshot hash #1258
### Fixed
- Pull only consecutive transactions chain for consensus #1241
- Handle transactions with positive fee correctly #1201
- Self avoiding walk: missing edges and negative 0d
- No active tips issue and consensus hanging

## [v2.8.3] - 2020-06-16
### Changed
- Whitelisting - change operator IP
### Fixed
- Broadcast joining height multiple times #1261
- Fixed missing parents metric

## [v2.8.2] - 2020-05-26
### Added
- Owner endpoint for observing majority heights
### Fixed
- Disallow offline nodes for communication
- Do not propose offline nodes for peer discovery

## [v2.8.1] - 2020-05-22
### Fixed
- Do not remove creating snapshot
- ChannelStorage database disabled 
- Increased timeouts for dead peers

## [v2.8.0] - 2020-05-21
### Added
- Additional metrics for consensus
- Network confirmation before marking a dead peer as offline
- Public last-tx-ref endpoint
### Fixed
- Accepting checkpoint blocks after (re)download

## [v2.7.1] - 2020-05-12
### Fixed
- Include own MajorityHeight when rewarding

## [v2.7.0] - 2020-05-12
### Added
- Endpoint returning last transaction reference for a given address
- Additional validation checking last transaction reference when submitting a transaction
- Rewards metrics

### Changed
- Rewards adjusted to 4 snapshots per minute
- Max snapshot heights for epochs adjusted accordingly to snapshot creation speed

### Fixed
- Reward only these nodes who sent the proposal for rewarded height
- Reduce number of meaningful snapshots to avoid downloading too much 
- Broadcast own joining height only after successful download
- Make double join impossible

## [v2.6.0] - 2020-05-08
### Changed
- Additional argument in Wallet CLI to pass already normalized tx amount 

## [v2.5.8] - 2020-05-01
### Fixed
- Missing parents will not be resolved if present locally and waiting for acceptance
- Don't broadcast finished blocks to offline peers
- Sort blocks topologically after redownload
- Remove unnecessary blocks from acceptance
- Remove unnecessary error from peer discovery 
- Disabled auto rejoin after node restart
- Can start only with no tmp directory

## [v2.5.7] - 2020-04-28
### Fixed
- Rewards amount

## [v2.5.6] - 2020-04-28
### Fixed
- Rewards distribution
- Observation and Transaction batch endpoints

## [v2.5.5] - 2020-04-27
### Fixed
- Make stored node proposals immutable
- Check for highest proposal when leaving #1096
- Last transaction reference hash
- Index oob error and reduce cpu utilization of SelfAvoidingWalk

## [v2.5.4] - 2020-04-22
### Added
- Sum balances for duplicated addresses in genesis data csv

## [v2.5.3] - 2020-04-22
### Added
- Config option to tell that allocated balances have been already normalized
### Fixed
- Validator for many overflowing transactions
- JVM metrics on alerting grafana
- Compare BuildInfo case classes instead of comparing hashes

## [v2.5.2] - 2020-04-20
### Added
- Additional logs for joining peer validator

## [v2.5.1] - 2020-04-17
### Fixed
- Remove old UDP code
- Transaction validator now validates signature properly

## [v2.5.0] - 2020-04-16
### Changed
- Update whitelisting

## [v2.4.0] - 2020-04-16
### Fixed
- Response signer turned on again with proper middleware order #1061
### Changed
- Validate hash of whole BuildInfo #713
- Update pl.abankowski.http4s-request-signer to 0.3.2

## [v2.3.0] - 2020-04-14
### Added
- http4s metrics and grafana dashboard
### Changed
- Peer discovery flow
- Marking peer as offline /w latest proposals
- Total connections and queue limit for http4s client

## [v2.2.1] - 2020-04-09
### Added
- Updated whitelisting for batch0
- Temporarily added batch1 to whitelisting
### Fixed
- Compile body to Array[Byte] only if request succeeded
- Updated BouncyCastle to 1.65 which fixes "algorithm not found" issue of keystore
- Temporarily turn off request signer

## [v2.2.0] - 2020-04-04
### Breaking changes
- Changed API endpoints visibility and security
- Owner API and dashboard available on port 9002
- Joining and leaving available only from owner API
### Added
- Http4s API and APIClient instead of akka-http #1020
- Owner API (located on port 9002)
- Request and response signing #992
- Transaction json (de)serialization methods in wallet
- New metrics for Grafana alerting #734
### Changed
- Double spend validation #1007
- Dropped support for json4s, used circe instead
- Changed API endpoints visiblity (ie. joining and leaving available only for node owner)
### Fixed
- Transaction max value validation #994
- Checking joining signature hash correctness #995
- Docker w/ whitelisting

## [v2.1.4] - 2020-03-27
### Changed
- Specify height and hash for rollback explicitly
### Fixed
- Make sure that we can't set leaving height smaller than joining height
- Broadcasting to and requesting only from ready peers

## [v2.1.3] - 2020-03-26
### Added
- Whitelisting file
### Fixed
- Make sure that ownJoinedHeight can be set only once
- Fix Docker releases in Github workflow

## [v2.1.2] - 2020-03-25
### Fixed
- Do not validate EigenTrust when doing the rollback

## [v2.1.1-rc] - 2020-03-25
### Fixed
- Do not store EigenTrust on disk and do not send it to cloud

## [v2.1.0-rc] - 2020-03-24
### Added
- Introduce rollback mode to start cluster (node) from specific point (snapshot) in time #820
- Wallet commands to display id and public key #976
- PeerAPI whitelisting #931
### Fixed
- Do not try to download snapshots if majority state is empty #935

## [v2.0.4-rc] - 2020-03-20
### Fixed
- Fix joining inside the majority interval #965

## [v2.0.3-rc] - 2020-03-19
### Fixed
- Marking peer as offline properly when peer has created no snapshots

## [v2.0.2-rc] - 2020-03-18
### Fixed
- Filter out offline peers for data resolving #947
- Fixed transaction hash integrity between wallet and node #946
- Historical join/leave heights #935

## [v2.0.1-rc] - 2020-03-17
### Changed
- Instantiate CloudStorage for Genesis flow

## [v2.0.0-rc] - 2020-03-17
### Added
- Changelog introduced
- GitHub Release workflow
