# rustcdc Documentation Index

Last Updated: May 24, 2026

This index maps the current, existing documentation set in this repository.
It intentionally avoids references to removed planning artifacts.

## Quick Navigation

### Getting Started
- [README.md](../README.md) - Project overview and build/test quickstart
- [getting_started.md](getting_started.md) - Local setup and first pipeline

### API and Architecture
- [api.md](api.md) - Public embedding API and runtime usage model
- [architecture.md](architecture.md) - Runtime architecture and correctness model
- [config_reference.md](config_reference.md) - Runtime and connector configuration

### Operations
- [runbook.md](runbook.md) - Operational procedures and recovery steps
- [troubleshooting.md](troubleshooting.md) - Failure diagnosis and fixes
- [deployment.md](deployment.md) - Deployment and environment guidance

### Reliability and Compatibility
- [reliability_testing.md](reliability_testing.md) - Reliability test strategy and evidence model
- [library_parity_matrix.md](library_parity_matrix.md) - Library-only parity and release gating

### Extension Surfaces
- [adapter_sdk.md](adapter_sdk.md) - Adapter conformance model
- [wasm_transform_sdk.md](wasm_transform_sdk.md) - WASM transform surface
- [wasm_conformance_contract.md](wasm_conformance_contract.md) - WASM conformance expectations

## Audience Map

- Maintainers and release committee: [library_parity_matrix.md](library_parity_matrix.md)
- Embedders/integrators: [api.md](api.md), [config_reference.md](config_reference.md)
- Operators/SRE: [runbook.md](runbook.md), [troubleshooting.md](troubleshooting.md), [deployment.md](deployment.md)
- Adapter/WASM authors: [adapter_sdk.md](adapter_sdk.md), [wasm_transform_sdk.md](wasm_transform_sdk.md)

## Maintenance Rules

1. Only link documents that currently exist in the repository.
2. Keep architecture and API claims synchronized with implementation and tests.
3. Update this index in the same PR when adding/removing top-level docs.
