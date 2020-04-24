# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]
### Fixed
- Make stored node proposals immutable

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
