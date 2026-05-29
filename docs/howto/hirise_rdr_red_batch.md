# Fetching HiRISE RDR `_RED` products with `plp fetch`

Cheat sheet for the most common ways to pull the RED color-band JP2 RDR
products. The dotted key is always `mro.hirise.rdr`. The product ID is
the HiRISE observation ID with the `_RED` suffix appended
(e.g. `PSP_003092_0985_RED`).

## 1. A single obsid on the command line

The simplest case — pass the full product ID directly:

```bash
plp fetch mro.hirise.rdr PSP_003092_0985_RED
```

The file lands under `{storage_root}/mro/hirise/rdr/PSP_003092_0985_RED/`.
Add `--here` to drop it into the current directory instead.

## 2. A handful of obsids on the command line

Variadic positional — pass each fully-qualified product ID. The batch
path engages automatically once there's more than one PID:

```bash
plp fetch mro.hirise.rdr PSP_003092_0985_RED ESP_089803_2650_RED
plp fetch mro.hirise.rdr PSP_003092_0985_RED ESP_089803_2650_RED ESP_089671_2650_RED
```

Downloads run in parallel (default 4 workers; tune with `--workers N`).
The per-PID outcome shows up as a `Fetch summary: …` line on stderr,
with any failures formatted as multi-line blocks (PID on its own line,
indented error below).

**Note:** `--pid-suffix _RED` is *not* applied to positional arguments —
that flag only activates with `--pids-from`. With positional PIDs you
need to spell out the full `_RED` suffix on each obsid yourself.

## 3. A whole CSV file

Your CSV column is named `observation_id` (lowercase). The auto-detector
in `pid_column` only knows the uppercase canonical names (`PRODUCT_ID`,
`FILE_NAME`, `IMAGE_ID`, `OBSERVATION_ID`), so it can't pick lowercase
`observation_id` automatically — you have to name the column with
`--pid-key`. Once the column is known, `--pid-suffix _RED` appends the
band to every obsid before the catalog lookup:

```bash
plp fetch mro.hirise.rdr \
  --pids-from Kolhar_summer_furrows_HiRISE-20260528053713.csv \
  --pid-key observation_id \
  --pid-suffix _RED
```

For CSVs where the PID column IS named with one of the canonical
uppercase names, you can drop `--pid-key`:

```bash
plp fetch mro.hirise.rdr --pids-from targets_with_PRODUCT_ID.csv --pid-suffix _RED
```

Useful options:

- `--workers 8` — increase parallel download threads (default 4).
- `--report full` — show one `OK …` or `FAIL …` block per PID on stdout,
  not just the errors on stderr.
- `--report jsonl` — emit one JSON object per PID; pipe into `jq` or
  another downstream tool.

## 4. Just the first few rows of a file (head pipe)

The `head` idiom for testing your command on a small slice before
committing to the full batch:

```bash
head -n 5 Kolhar_summer_furrows_HiRISE-20260528053713.csv \
  | plp fetch mro.hirise.rdr \
      --pids-from - \
      --pid-key observation_id \
      --pid-suffix _RED
```

`--pids-from -` reads from stdin. The first-line comma sniff
auto-detects this as CSV (since the header line contains commas);
`--pid-key observation_id` is still needed because of the lowercase
column name. `--pid-suffix _RED` applies as in case 3.

The same idiom works with other shell filters:

```bash
# First 10 obsids after sorting by date column:
sort -t, -k7 Kolhar_summer_furrows_HiRISE-20260528053713.csv \
  | head -n 10 \
  | plp fetch mro.hirise.rdr --pids-from - --pid-key observation_id --pid-suffix _RED

# Only the COMPLETE-status rows:
grep COMPLETE Kolhar_summer_furrows_HiRISE-20260528053713.csv \
  | plp fetch mro.hirise.rdr --pids-from - --pid-key observation_id --pid-suffix _RED
```

## Recap — when do you need which flag?

| Situation | `--pids-from` | `--pid-key` | `--pid-suffix` |
|---|---|---|---|
| 1 obsid on the command line | — | — | — (write `_RED` into the PID) |
| N obsids on the command line | — | — | — (write `_RED` into each PID) |
| CSV file, column named `PRODUCT_ID` (or other canonical) | path to file | — | `_RED` |
| CSV file, column named anything else (e.g. `observation_id`) | path to file | column name | `_RED` |
| Pipe a slice of a CSV via `head` / `grep` / etc. | `-` | same as the file case | same as the file case |
