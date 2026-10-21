# Want to Retire

A year-long, pre-registered forward test of LLM trading decisions in crypto markets. The evaluation spec is frozen and externally timestamped before launch. LLM, numeric and hybrid arms run side by side on the same data, execution and risk layers. A hash-chain head is published and externally anchored every day, so anyone can check that results were not rewritten after the fact.

> Research demonstration only. Not investment advice.
> 研究演示，不构成投资建议。

Status: design phase. No forward test is running yet, and the evaluation spec has not been frozen.

## Why a forward test

An LLM cannot be backtested fairly. Its training data already contains the market history that a backtest replays, so a good backtest may only show what the model remembers. A clean test has to run forward: the model makes each decision live, on data that did not exist when it was trained, and the results are judged by rules written down before any result exists.

The system itself must not leak the future either. Every data record carries the time it became available, and a decision made at time *t* can only see records that were available by *t*.

## Experiment

Five arms share the same data, simulator, fee model, risk limits and log. Only the decision module differs.

| Arm | Decision maker |
|---|---|
| A0 | Buy and hold |
| A | Numeric strategy (gplearn, LightGBM, hand-built factors), no LLM |
| B | An LLM decides directly |
| C | Numeric signals, with an LLM that can veto or adjust them |
| J | Arm A's target weights, applied only when a probability model (Jev) puts the chance that the rebalance beats holding above a preset threshold |

B and C use a DeepSeek model through its public API, as released and without fine-tuning. The API does not pin model versions, so each call records the version identifiers the API returns, and a daily probe watches for silent model changes. The main results describe each arm as it actually ran, including any model changes made by the provider; results for individual model versions are exploratory. Every arm returns the same output: target portfolio weights, a rationale and a confidence ([ADR 0001](docs/adr/0001-decision-output-target-weights.md)).

## Safeguards

| Safeguard | What it prevents |
|---|---|
| A decision at time *t* gets a snapshot of the data available by *t*, and nothing else ([ADR 0002](docs/adr/0002-point-in-time-data.md)) | The system leaking future data |
| Decision modules receive a snapshot and return a decision. They cannot modify the risk limits, the log or the evaluation code | A strategy, or a later self-improvement loop, changing how it is judged |
| Every event is appended to a hash chain, and the chain head is published and externally timestamped every day | Results being edited after the fact without anyone noticing |
| The evaluation spec is frozen and externally timestamped before the forward test starts, and is not changed afterwards | Metrics chosen after seeing the results |
| A running strategy never changes itself. An improvement becomes a new frozen version and enters the test as a new candidate | Results that mix versions |

## Scope of v1

Paper trading on public market data. No real money and no exchange keys. The trading system is not reachable from the internet, and the public dashboard only reads sanitized snapshots.

## Design records

Decisions are recorded in [`docs/adr/`](docs/adr/), one decision per file.
