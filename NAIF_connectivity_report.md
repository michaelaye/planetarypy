# NAIF server connectivity timeout — incident report

**Prepared:** 2026-06-13
**Reporter:** K.-Michael Aye (planetarypy maintainer), kmichael.aye@gmail.com
**Affected host:** `naif.jpl.nasa.gov` (HTTPS / port 443)

## Summary

During an automated CI run, our client was **unable to establish a TCP connection** to
`naif.jpl.nasa.gov` to download a small set of generic SPICE kernels. Every attempt over a
~15-minute window failed with `[Errno 110] Connection timed out` (a TCP connect timeout — no
response to the connection attempt). The same download succeeded normally on a retry ~7 hours
later, so the condition was **transient**. We are reporting it because this class of timeout
against `naif.jpl.nasa.gov` recurs intermittently for us from cloud-hosted CI runners, and the
pattern (connect timeout rather than HTTP error, DNS failure, or refused connection) may point to
a server-side load, rate-limit, or network-reachability condition worth investigating.

## What was being requested

The client was fetching 5 small files from the public generic-kernels tree
(`https://naif.jpl.nasa.gov/pub/naif/generic_kernels/`):

| URL | Approx size |
|-----|-------------|
| https://naif.jpl.nasa.gov/pub/naif/generic_kernels/lsk/naif0012.tls | ~5 KB |
| https://naif.jpl.nasa.gov/pub/naif/generic_kernels/pck/pck00010.tpc | ~120 KB |
| https://naif.jpl.nasa.gov/pub/naif/generic_kernels/pck/de-403-masses.tpc | ~5 KB |
| https://naif.jpl.nasa.gov/pub/naif/generic_kernels/spk/planets/de432s.bsp | ~10 MB |
| https://naif.jpl.nasa.gov/pub/naif/generic_kernels/spk/satellites/mar099s.bsp | ~10 MB |

Total payload is ~20 MB — a routine, small request.

## Symptom / error

The download client (Python `urllib`) raised, on each attempt:

```
TimeoutError: [Errno 110] Connection timed out
urllib.error.URLError: <urlopen error [Errno 110] Connection timed out>
```

`[Errno 110] Connection timed out` is a **TCP-level connect timeout**: the client sent connection
requests (SYN) to `naif.jpl.nasa.gov:443` and received no response within the socket timeout. This
is distinct from — and rules out, from the client's side:

- **DNS failure** (would be "Name or service not known"),
- **Connection refused** (`[Errno 111]` / ECONNREFUSED — server up but port closed),
- **HTTP error** (a 4xx/5xx status — connection succeeded but request rejected),
- **TLS/SSL error** (handshake-level failure).

So the server (or a network device in front of it) was not completing the TCP handshake for our
client during the window.

## Timeline (UTC, 2026-06-13)

The client retries with exponential backoff (5 attempts). All five failed:

| Time (UTC) | Event |
|------------|-------|
| 00:23:46 | Prefetch begins; first connection attempt to `naif.jpl.nasa.gov` |
| 00:26:02 | Attempt 1 → `Connection timed out` (retry in 15 s) |
| 00:28:33 | Attempt 2 → `Connection timed out` (retry in 30 s) |
| 00:31:17 | Attempt 3 → `Connection timed out` (retry in 60 s) |
| 00:34:34 | Attempt 4 → `Connection timed out` (retry in 120 s) |
| 00:38:48 | Attempt 5 → `Connection timed out`; **gave up after 5 attempts** |
| ~07:51 | Re-run of the same job **succeeded** — downloads completed normally |

Observed outage window from our vantage point: **at least 00:23–00:39 UTC** (≈15 min of
continuous connect timeouts). Service had recovered by ~07:51 UTC.

## Observed frequency

This is not a one-off: across a recent ~2-week window of our CI (2026-05-29 to 2026-06-13),
connect-timeouts to `naif.jpl.nasa.gov` during this generic-kernel prefetch were the **single most
common cause of CI failure** — at least **5 occurrences on ~4 distinct days**, and that is an
**undercount**, because runs we manually re-ran to success are not tallied as failures.

## Client environment

- **Origin:** GitHub-hosted Actions runners (`ubuntu-latest`). These run in **Microsoft Azure**
  IP ranges and are **ephemeral** (the source IP varies run-to-run). If NAIF applies per-IP or
  per-ASN rate-limiting / filtering, traffic from Azure/GitHub-Actions ranges may be the relevant
  dimension.
- **Client library:** Python `urllib` (`urlopen`), default User-Agent `Python-urllib/3.11`.
- **Protocol:** HTTPS, port 443.
- **Request profile:** 5 sequential small GETs (~20 MB total), no unusual concurrency.

## Why we think it may be worth a look

- It is a **connect-level timeout** (no SYN-ACK), not an application error — consistent with
  server overload, an upstream network/routing issue, or a firewall/rate-limiter silently dropping
  packets from cloud-runner IP ranges, rather than with a missing file or a bad request.
- It is **intermittent and recurring**: we see this category of timeout against
  `naif.jpl.nasa.gov` (and, separately, some PDS data nodes) sporadically from CI, and it clears on
  its own within hours. Single occurrences are easy to dismiss as transient, but the recurrence is
  what prompts this report.
- The affected files are tiny, frequently-requested generic kernels — so this likely affects many
  automated consumers, not just us.

## Questions that would help us (and others)

1. Are there known intermittent availability windows / maintenance, or load-shedding, on
   `naif.jpl.nasa.gov` around the times above (≈00:23–00:39 UTC, 2026-06-13)?
2. Is there any rate-limiting or IP/ASN-based filtering that could intermittently drop connections
   from cloud-CI (Azure / GitHub Actions) IP ranges?
3. Is there a recommended **mirror or CDN** for the generic kernels for automated/CI consumers, to
   reduce load on the primary host (and improve our reliability)?

## How this surfaced

`planetarypy` (an open-source planetary-science Python package) prefetches these generic kernels in
its CI test setup. The failure was a CI infrastructure timeout only — no data corruption, and the
retry succeeded — so there is no urgency. We are sharing the specifics in case they are useful for
NAIF operations.

---
*Contact for follow-up: kmichael.aye@gmail.com*
