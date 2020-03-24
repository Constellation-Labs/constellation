# Changelog
All notable changes to this project will be documented in this file.

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
