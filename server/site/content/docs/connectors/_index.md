+++
title = "Connectors"
description = "Source connector guides for rustcdc: PostgreSQL logical replication, MySQL and MariaDB binlog, and SQL Server CDC change tables."
weight = 50
sort_by = "weight"
template = "section.html"
page_template = "page.html"
+++

Each connector reads change events natively — PostgreSQL through logical replication,
MySQL and MariaDB through the binary log, SQL Server through CDC change tables — and
produces the same [event model](@/docs/concepts.md#1-event-model), so a sink or transform
written against one source works unchanged against the others.

| Connector | Mechanism | Positioning |
|---|---|---|
| [PostgreSQL](@/docs/connectors/postgres.md) | Logical replication (`pgoutput`) | LSN |
| [MySQL / MariaDB](@/docs/connectors/mysql.md) | Binary log, row format | GTID or file + offset |
| [SQL Server](@/docs/connectors/sqlserver.md) | CDC change tables | LSN |

Read the guide for your database before the first production run: each has prerequisites
that must be set on the server itself, and one of them — PostgreSQL's `REPLICA IDENTITY` —
silently changes what a `DELETE` event contains if it is left at its default.
