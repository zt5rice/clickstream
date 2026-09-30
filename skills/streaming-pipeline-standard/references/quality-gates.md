# Quality gates reference

## Layering

Each layer catches a different class of failure. Skipping a layer does not save time; it
moves the failure later.

| Layer | Catches | Example |
|---|---|---|
| Schema validation at ingest | Malformed or unexpected-shape records | Reject to DLQ with a reason code |
| Unit tests | Logic errors in pure functions | Serialization, validation, aggregation helpers |
| Integration tests | Wiring against real dependencies | Producer → broker → sink round trip |
| Model tests (declarative) | Data-shape assumptions | Uniqueness, not-null, accepted values |
| Independent quality checks | Assumptions the model layer does not express | Cross-source reconciliation, volume anomalies |
| End-to-end smoke | The whole path still works | Upload → process → read API returns a value |

## Why two independent quality frameworks

A single framework only catches what you thought to express in it. Running a second,
independent checker over the same data surfaces different mistakes — and disagreements
between the two are themselves a signal. Merge both into **one report** so a reader does
not have to reconcile two dashboards.

## The gate

```
lint → format check → tests
```

Expose it as a single command so it is trivially runnable before every commit and in CI.
A gate that requires remembering three commands will be skipped.

CI shape:

```
lint → unit → integration → build images → (optional) deploy
```

Deploy stays optional and manual until the earlier stages are trustworthy.

## Test hygiene

- Integration tests must be **repeatable**: unique fixture identifiers plus cleanup, so a
  second run in the same session behaves like the first.
- Keep a fast subset for CI and mark slow tests explicitly, so the default gate stays
  seconds not minutes.
- A test that has never failed is untested. Break the thing deliberately once to confirm
  the test actually detects it.

## Evidence discipline

Every published number must be traceable to a run you can reproduce:

- Store raw results as artifacts (JSON in the repo) next to the summary.
- Put the reproduction command next to the number.
- Never publish an estimate as a measurement. Mark estimates as estimates.

