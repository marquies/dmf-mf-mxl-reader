# Changelog

## [1.0.0-rc.8](https://github.com/marquies/dmf-mf-mxl-reader/compare/v1.0.0-rc.7...v1.0.0-rc.8) (2026-10-26)


### Features

* show the id of the flow each ring reads ([#16](https://github.com/marquies/dmf-mf-mxl-reader/issues/16)) ([1c6c1f3](https://github.com/marquies/dmf-mf-mxl-reader/commit/1c6c1f3001bdf981f4577ab987ac2adac5b75ba2))


### Bug Fixes

* **deps:** move onto go-mxl 1.1.0-rc.7 and its libmxl ([#15](https://github.com/marquies/dmf-mf-mxl-reader/issues/15)) ([79aff98](https://github.com/marquies/dmf-mf-mxl-reader/commit/79aff9865350a3eff878097b1f63010ab45f2f45))

## [1.0.0-rc.7](https://github.com/marquies/dmf-mf-mxl-reader/compare/v1.0.0-rc.6...v1.0.0-rc.7) (2026-10-26)


### Features

* count and check every read with libmxl-monitoring ([e0fdffd](https://github.com/marquies/dmf-mf-mxl-reader/commit/e0fdffd01abb694bf985b3f08733be6ed6d87fbd))
* count and check every read with libmxl-monitoring ([9c056ac](https://github.com/marquies/dmf-mf-mxl-reader/commit/9c056ace0dc5ae120227819e4dfc886868ca22cb))


### Build System

* pin libmxl-monitoring 0.2.0 by commit ([ca6e5d5](https://github.com/marquies/dmf-mf-mxl-reader/commit/ca6e5d5397e077e503b6bacc6cd233acfaf4f633))

## [1.0.0-rc.6](https://github.com/marquies/dmf-mf-mxl-reader/compare/v1.0.0-rc.5...v1.0.0-rc.6) (2026-10-22)


### Features

* **nmos:** connect reader slots to flows over BCP-007-03 ([8d0471a](https://github.com/marquies/dmf-mf-mxl-reader/commit/8d0471a5fba680d5ed7f5d7cca0716110d13ff19))
* **nmos:** connect reader slots to flows over BCP-007-03 ([44f196b](https://github.com/marquies/dmf-mf-mxl-reader/commit/44f196bf84c1b0f65d2b2f0f63e9aa03995a3d2b))

## [1.0.0-rc.5](https://github.com/marquies/dmf-mf-mxl-reader/compare/v1.0.0-rc.4...v1.0.0-rc.5) (2026-10-22)


### Bug Fixes

* read a whole request before answering it ([72ed0fc](https://github.com/marquies/dmf-mf-mxl-reader/commit/72ed0fce793b40abffaaac18413ee77de38296ca))
* read a whole request before answering it ([52d96fd](https://github.com/marquies/dmf-mf-mxl-reader/commit/52d96fd6b03ad0f3aac89da9724f0bb47f9e5246))

## [1.0.0-rc.4](https://github.com/marquies/dmf-mf-mxl-reader/compare/v1.0.0-rc.3...v1.0.0-rc.4) (2026-10-20)


### Features

* show the ring's size and draw a grain into every slot ([0f6c432](https://github.com/marquies/dmf-mf-mxl-reader/commit/0f6c432d62820940905442233af26f4c0cfe4e1d))
* show the ring's slots, slot size and total size ([69a7002](https://github.com/marquies/dmf-mf-mxl-reader/commit/69a7002c7edec2ee2fc2e428efd0de3b77fb8894))


### Bug Fixes

* draw a grain into every slot of the ring view ([dbdfe5f](https://github.com/marquies/dmf-mf-mxl-reader/commit/dbdfe5f821b196314dc5e6a352ad95a6d7524939))

## [1.0.0-rc.3](https://github.com/marquies/dmf-mf-mxl-reader/compare/v1.0.0-rc.2...v1.0.0-rc.3) (2026-10-20)


### Features

* name the reader's version and libmxl at start and on --version ([96ec4ea](https://github.com/marquies/dmf-mf-mxl-reader/commit/96ec4eae00d0d1a668ae6046ed7dcef0b0c8c19b))
* name the reader's version and libmxl at start and on --version ([06c1620](https://github.com/marquies/dmf-mf-mxl-reader/commit/06c1620570729eeb2812287fa88f12fd0f2a0e59))
* show each flow's ring and where its grains arrive, live ([65bff6d](https://github.com/marquies/dmf-mf-mxl-reader/commit/65bff6d64c9fdcfec7806d513cae2e54e48550cd))
* show each flow's ring and where its grains arrive, live ([b5ef374](https://github.com/marquies/dmf-mf-mxl-reader/commit/b5ef3741bbdb4e531c90885baa96022c07659b1d))

## [1.0.0-rc.2](https://github.com/marquies/dmf-mf-mxl-reader/compare/v1.0.0-rc.1...v1.0.0-rc.2) (2026-10-20)


### Features

* classify every read and count each outcome ([4c3c605](https://github.com/marquies/dmf-mf-mxl-reader/commit/4c3c605497080341110a6a2399109a4d7309a357))
* classify every read and count each outcome ([c2ff42f](https://github.com/marquies/dmf-mf-mxl-reader/commit/c2ff42ff4f01375fa106f0b9b601feadb4da06b4))


### Bug Fixes

* count a stale ring slot as missing and a partial grain as incomplete ([60cdc3c](https://github.com/marquies/dmf-mf-mxl-reader/commit/60cdc3c3bbf59e1d364380e56f6d9c8feea381f8))

## [1.0.0-rc.1](https://github.com/marquies/dmf-mf-mxl-reader/compare/v1.0.0-rc.0...v1.0.0-rc.1) (2026-10-19)


### Features

* read MXL flows at the head and report delivery ([13308c6](https://github.com/marquies/dmf-mf-mxl-reader/commit/13308c6da827d4249840a12a8591b20e8d3873cd))


### Bug Fixes

* report delivery on its own path and count stalls per timeout ([933ef42](https://github.com/marquies/dmf-mf-mxl-reader/commit/933ef42407ff79eb4f3f574baa347aaf21f93b80))


### Continuous Integration

* build under the repository's own name and with the release ([4cb6397](https://github.com/marquies/dmf-mf-mxl-reader/commit/4cb6397b03e05391ffadaa076ec706dec09243bd))
