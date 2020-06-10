# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]

## [v2.9.0-rc1] - 2020-06-10
## Changed
- Circe update to v0.13.0
- Get rid of circe auto derivation
- Whitelisting - change operator IP
- Data resolver refactor
## Fixed
- Pull only consecutive transactions chain for consensus #1241
- Handle transactions with positive fee correctly #1201

## [v2.8.4] - 2020-06-04
## Added
- Dead peer caching #1239
## Changed
- Different reputation does not change snapshot hash #1258
- Broadcast joining height multiple times #1261

## [v2.8.3] - 2020-05-29
## Fixed
- Self avoiding walk: missing edges and negative 0d
- No active tips issue and consensus hanging

## [v2.8.2] - 2020-05-26
## Added
- Owner endpoint for observing majority heights
## Fixed
- Disallow offline nodes for communication
- Do not propose offline nodes for peer discovery

## [v2.8.1] - 2020-05-22
## Fixed
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
