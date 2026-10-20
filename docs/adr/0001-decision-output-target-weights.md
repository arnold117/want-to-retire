# ADR 0001: Decision modules output target weights

Status: Accepted

## Context

The four arms (A0, A, B, C) share one simulator, one fee model and one set of risk limits. The comparison only means something if every arm hands those shared layers the same kind of output. The LLM arms should also not have to reason about order mechanics.

## Decision

Every decision module returns three fields:

- `weights`: target portfolio weights by symbol, with cash as one of the entries
- `rationale`: text explaining the decision
- `confidence`: the module's own confidence in the decision

Weights are non-negative and sum to 1 including cash. This assumes spot trading. If the simulator moves to perpetual futures, this record has to be revisited.

The execution layer turns the gap between target and current weights into orders. Execution hints, such as limit prices or timing, are left out for now.

## Consequences

- The arms are directly comparable, and risk limits and execution are written once.
- An LLM only has to choose direction and size.
- Execution intent, such as resting a limit order or waiting for a better price, cannot be expressed.
- Adding an optional hints field later is cheap. Switching to order-level output would mean rewriting the execution layer and every arm's adapter.
