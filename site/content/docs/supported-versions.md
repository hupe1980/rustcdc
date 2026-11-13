+++
title = "Supported versions"
description = "The database and broker versions every release is tested against, and the support windows they follow."
weight = 15
+++

Every release runs its container suites against these versions. The full suites (snapshot,
stream, snapshot-to-stream handoff, type fidelity, process-crash replay) run on the newest; every
other version runs connect, capture and close.

| Engine | Full suites | Also tested | Vendor support window, read 2026-11-12 |
|---|---|---|---|
| PostgreSQL | 18 | 15, 16, 17 | [Five years per major](https://www.postgresql.org/support/versioning/): 15 to 2027-11-11 … 18 to 2030-11-14 |
| MySQL | 9.7 LTS | 8.4 LTS | [LTS releases](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html): 8.4 to 2032-04-30, 9.7 to 2034-04-30 |
| MariaDB | 12.3 | 10.11, 11.4, 11.8 | [Long-term releases](https://mariadb.org/about/#maintenance-policy): 10.11 to 2028-02-16 … 12.3 to 2029-06-12 |
| SQL Server | 2025 | 2017, 2019, 2022 | [Fixed lifecycle](https://learn.microsoft.com/lifecycle/products/?terms=sql%20server), extended support: 2017 to 2027-10-12 … 2025 to 2036-01-06 |
| Apache Kafka | 4.4.0 | — | The newest release (sink) |
| Redpanda | 26.2.6 | — | The newest release (sink); not tracked by the window check |

MySQL Innovation releases (26.x) and MariaDB rolling releases (13.x) are supported by their vendors
for months, not years, and are not tested.

## Older and newer releases

- **PostgreSQL** before 15, **MySQL** 8.0 and **MariaDB** before 10.11 are out of vendor support.
  rustcdc does not refuse them, and does not test them.
- **SQL Server** 2016 is accepted at startup and not tested; 2014 and earlier are refused.
- **A newer major** than the table lists is untested. When a vendor releases one, the window
  check fails until it is added, and the newest tested release runs the full suites.

## How this stays current

The versions live in one file, `crates/rustcdc/tests/support/matrix.txt`, which every suite and CI
job reads. `cargo xtask support-window` compares it with the vendor windows and names what to add,
drop or promote; it runs before every release. The policy gate keeps this page in step with the
file.

To run any suite against another version without an edit:

```sh
RUSTCDC_TEST_POSTGRES=17-alpine CDC_RS_RUN_DOCKER_TESTS=1 \
  cargo test -p rustcdc --features postgres --test postgres_stream_integration
```
