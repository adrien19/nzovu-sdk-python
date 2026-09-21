# Historical SDK changelog

Retained from the source package; these are not Nzovu package releases.

# Changelog

All notable changes to the Chronoqueue Python SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive Makefile with development and CI/CD targets
- New Makefile targets: `lock` and `update` for dependency management
- GitHub Actions CI/CD workflows for automated testing and releases
- Code quality tools configuration (Black, isort, flake8, mypy)
- Contribution guidelines (CONTRIBUTING.md)
- Pull request and issue templates
- Test coverage reporting setup
- Multi-version Python testing (3.10, 3.11, 3.12, 3.13, 3.14)
- Quick reference guide for common commands

### Changed
- Migrated from shell script to Makefile for proto generation
- Updated dependency versions to modern, compatible releases
- Enhanced .gitignore for better project hygiene
- Improved README with CI/CD documentation

### Deprecated
- `generate_proto_classes_from_github.sh` (replaced by `make gen-proto`)

## [0.1.0] - 2025-10-24

### Added
- Initial release of Chronoqueue Python SDK
- gRPC client implementation
- Basic queue operations (create, delete, post message)
- SSL/TLS support
- Unit test framework
- Proto file definitions

[Unreleased]: https://github.com/adrien19/chronoqueue_sdk/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/adrien19/chronoqueue_sdk/releases/tag/v0.1.0
