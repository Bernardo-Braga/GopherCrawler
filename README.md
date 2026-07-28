# GopherCrawler

A single-threaded [Gopher protocol](https://datatracker.ietf.org/doc/html/rfc1436)
crawler written in C. Point it at a Gopher server and it walks the entire menu
tree under that server, fetching every directory and file it can reach, and
prints a report of what it found.

The whole thing is built on **raw BSD sockets** — `socket()`, `connect()`,
`send()`, `recv()`, `select()` and `getaddrinfo()` straight from the POSIX
networking API. There is no HTTP library, no `libcurl`, and no third-party
`requests`-style client; the Gopher request/response exchange is spoken byte for
byte over a plain TCP stream.

## Building

```sh
gcc gopher_client.c -o gopher_client
```

Requires only a POSIX C environment (the source defines `_POSIX_C_SOURCE`
`200112L`). No external dependencies.

## Running

```sh
./gopher_client hostname:port
```

For example:

```sh
./gopher_client comp3310.ddns.net:70
```

The argument must be a single `hostname:port` string (port is required, `1`–
`65535`). Progress is logged to stdout as the crawl runs, followed by a summary
report at the end. To capture a run:

```sh
./gopher_client comp3310.ddns.net:70 > output.txt
```

## How it works

- **Breadth-first traversal.** Directories are held in an FIFO queue. The root
  selector (`""`) is seeded first, then every type-`1` menu item that points
  back at the *same* host and port is enqueued. A set of "seen" keys
  (`type|host|port|selector`) prevents the crawler from visiting the same
  directory or re-fetching the same file twice, which keeps it from looping on
  self-referential menus such as `/maze`.
- **Item handling.** For each menu line the type character decides what happens:
  type `1` is queued as a directory, type `0` is fetched and its decoded/raw text
  size recorded, binary types (`4 5 6 9 g I s`) are fetched and sized, type `3`
  is recorded as an invalid reference, and known-but-unsupported types
  (`7 8 T + h`) plus any unknown types are logged as issues rather than crawled.
- **Same-server scoping.** Any reference to a different host or port is *not*
  crawled. Instead the crawler makes a single TCP connection to that external
  server to check whether it is reachable, and records it as UP or DOWN.
- **Robust fetching.** Each connection uses a non-blocking `connect()` with a
  `select()`-based connect timeout, per-socket send/receive timeouts, and an
  overall 20-second fetch deadline so slow-drip / tarpit selectors can't stall
  the crawl forever. Transient connect timeouts are retried with exponential
  backoff (directories get up to 5 attempts, files up to 3); a refused
  connection is treated as permanent and not retried. Hard size caps guard
  against servers that push unbounded data.
- **Text decoding.** Text files are decoded per the Gopher rules: the lone `.`
  line marks the terminator, and leading-dot lines are un-doubled
  (`..` → `.`). Both the decoded size and the raw byte count are reported, and a
  missing terminator line is flagged as an issue.

## Understanding `output.txt`

`output.txt` is a captured run of the crawler against `comp3310.ddns.net:70`
(the COMP3310 test server). It has two parts.

**1. The live request log** — one line per network action as the crawl
progresses, each stamped with a wall-clock time:

```
[21:58:14] REQUEST comp3310.ddns.net:70 "/acme/about\r\n"
[21:58:19] RETRY 1/3 comp3310.ddns.net:70 "/acme/about\r\n" - reason: connect timed out (waiting 500 ms)
```

- `REQUEST` lines show the exact selector sent to the server, with the trailing
  `\r\n` terminator printed literally so you can see the raw bytes on the wire.
- `RETRY` lines appear when a connect times out; they show the attempt number,
  the reason, and the backoff delay before the next try.

**2. The `=== Crawl Summary ===`** — printed once the queue drains. For this run
it reports:

- **Gopher directories found: 41** — every directory the crawler visited,
  including the root `/`. (The root is intentionally counted; see the note in the
  source.) This includes the `/maze/*` and `/misc/nest*` chains the test server
  uses to exercise deep and self-referential trees.
- **Simple text files found: 12** — each type-`0` file with its *decoded* size
  and its *raw* on-the-wire size (they differ because of `\r\n` line endings and
  dot-unescaping). E.g. `/rfc1436.txt (36494 bytes decoded, 37396 bytes raw)`.
- **Binary/non-text files found: 2** — `/misc/binary` and
  `/misc/encabulator.jpeg`, sized in raw bytes.
- **Smallest text / binary file** — the minimum-size file in each category
  (`/misc/empty.txt` at 0 decoded bytes, and `/misc/binary` at 253 bytes).
- **External servers referenced: 5** — hosts/ports pointed to by menu items but
  outside the start server, each probed once and marked UP or DOWN with the
  reason. This run shows `gopher.floodgap.com:70` and `comp3310.ddns.net:71` UP,
  and ports `72`/`73`/`74` DOWN (refused, or timed out after retries).
- **Invalid references, type 3: 1** — a Gopher error item the server returned
  (`I don't have the resource you asked for.`).
- **Other issues/errors: 5** — everything the crawler couldn't fully process:
  malformed menu lines, a text file missing its terminator (`/misc/malformed2`),
  and the deliberately slow selectors (`/misc/firehose`, `/misc/tarpit`,
  `/misc/godot`) that hit the 20-second fetch deadline.

In short, `output.txt` is both a play-by-play of every request the crawler made
and a final inventory of the server's Gopher hole.

## Files

- `gopher_client.c` — the crawler source (single file).
- `gopher_client` — a compiled binary.
- `output.txt` — a captured run against `comp3310.ddns.net:70`.
