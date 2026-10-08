# sr-compare

Validates that two Confluent-compatible Schema Registry instances hold
**identical data**, via their HTTP APIs only — never touches Kafka
directly. Reports precise, kind-tagged mismatches rather than a bare
pass/fail, so a failure tells you exactly what differs and where.

## What it does

For every subject present on either side (including soft-deleted
subjects, via `?deleted=true`), it compares:

- **Subjects and versions** — set differences in both directions.
- **Schema content** per (subject, version) — exact byte comparison, with
  a JSON-semantic fallback so a purely cosmetic formatting difference
  (e.g. field order, whitespace) is reported distinctly from a real
  content difference.
- **Schema ID, schema type, references** per (subject, version).
- **Schema metadata and `ruleSet`** (Confluent's Data Contracts fields),
  compared by parsed JSON equality.
- **The global schema-ID index** (`GET /schemas/ids/{id}`) — a second,
  independent lookup path through each registry's storage, checked for
  every ID seen while walking subjects/versions, including its own
  metadata/`ruleSet`.
- **`latest` resolution** (`GET .../versions/latest`) — a distinct code
  path from listing + fetching versions individually, especially
  relevant around soft-deleted subjects.
- **Global and per-subject `config`** (compatibility level, plus the
  config-level `defaultMetadata`/`overrideMetadata`/`defaultRuleSet`/
  `overrideRuleSet` fields) **and `mode`.**
- **Contexts** (`GET /contexts`) — enumerates and compares per-context if
  the server supports it, falling back to default-context-only otherwise.

It walks subjects concurrently with a worker pool (bounded by
`-concurrency`), retries transient connection errors with backoff (useful
against something like a `kubectl port-forward` tunnel under load), and
reuses HTTP connections.

Exit code is `0` when identical, `1` if any mismatch was found — usable
as a CI-style gate, not just an interactive report.

## Requirements

- Go 1.23+ to build. No external dependencies (standard library only).
- Network access to both Schema Registry HTTP endpoints.
- Read credentials for both, if they require auth.

## Building

```bash
go build -o sr-compare .
```

## Running

```bash
./sr-compare -a http://localhost:28081 -b http://localhost:38081
```

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `-a` / `-b` | — (required) | Base URLs of the two Schema Registry instances to compare |
| `-concurrency` | `8` | Subjects (and schema IDs) compared concurrently |
| `-timeout` | `30s` | Per-request HTTP timeout |
| `-v` | `false` | Print progress while checking subjects |
| `-tls-ca` | — | Path to a CA certificate (PEM) to trust, in addition to system roots (applies to both `-a` and `-b`) |
| `-tls-cert` / `-tls-key` | — | Client certificate/key (PEM), for mTLS (applies to both sides) |
| `-tls-insecure-skip-verify` | `false` | Skip TLS certificate verification (test/dev only) |
| `-a-user` / `-a-pass` | — | HTTP Basic Auth credentials for A |
| `-b-user` / `-b-pass` | — | HTTP Basic Auth credentials for B |

TLS is otherwise automatic: just use an `https://` URL for `-a`/`-b`. The
`-tls-*` flags only matter when you need a non-default CA, mTLS, or to
skip verification. Credentials are deliberately **per side** — `-a` and
`-b` are frequently different environments with different credentials,
even when they share the same CA.

### Examples

Plaintext, no auth (the common case comparing two local test clusters):

```bash
./sr-compare -a http://localhost:28081 -b http://localhost:38081 -v
```

TLS with HTTP Basic Auth, different credentials on each side:

```bash
./sr-compare \
  -a https://sr-a.example.com:8081 -a-user userA -a-pass passA \
  -b https://sr-b.example.com:8081 -b-user userB -b-pass passB \
  -tls-ca /path/to/ca.pem
```

## Output

```
A = http://localhost:28081
B = http://localhost:38081

Contexts: A=[.] (GET /contexts supported=true)  B=[.] (supported=true)

Subjects: A=1183 B=1183 (compared=1183)
Versions checked: 13476
Distinct schema IDs checked (global index): 8621
Mismatches: 0

RESULT: IDENTICAL
```

When there are differences, it instead prints a count by mismatch kind
(e.g. `missing-subject-in-b`, `schema-mismatch`, `id-mismatch`,
`metadata-mismatch`, `subject-config-mismatch`) followed by a detailed
listing of each one, capped at 200 entries.
