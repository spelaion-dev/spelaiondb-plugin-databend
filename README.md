# spelaiondb-plugin-databend

Databend for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop database
client. The engine's driver, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This is the one for Databend. It is built here, in its own repository, and
SpelaionDB pins by hash the exact release it was tested with (its ADR-042, ADR-055).

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.1. Experimental until SpelaionDB 1.0.
- **Engine:** `databend`, over Databend's HTTP query API (`/v1/query`) with `hyper` and rustls,
  user and password in HTTP Basic.
  - Results are read page by page through `next_uri`, and a cancel kills the query on the server.
  - The session (the database, the settings, the open transaction) is kept by the client and sent back with every request, so `USE` and `BEGIN … COMMIT` work as in any engine.
  - Each value is typed by its column: numbers, booleans, dates and timestamps are typed columns; `Variant` is JSON; tuples, maps and the rest are the server's text.
  - The explorer shows the databases, and in each its tables and views.
- **What it does not do:** read-only connections and agents (Databend enforces no read-only mode), editing rows (no primary keys), bulk imports, bound values, plans. A Databend cluster that needs sticky routing for a transaction is not handled yet.
- **License:** Apache-2.0.

## Working on it

```bash
ci/services.sh
```

starts the test servers (Databend 1.2.881 in Docker, and the same behind a TLS proxy, with their
keys). Then

```bash
SPELAION_TEST_DATABEND=127.0.0.1:18310 SPELAION_TEST_DATABEND_TLS=127.0.0.1:18443 cargo test
```

runs the type zoo, the session and its transactions, batches, cancel, the explorer, the password,
TLS and the protocol's conformance set against this plugin's own program. Without the variables
the tests that need a server are skipped, which does not count as tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_DATABEND=/path/to/target/debug/spelaiondb-plugin-databend`, in
SpelaionDB's repository; a release build ignores that variable).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
