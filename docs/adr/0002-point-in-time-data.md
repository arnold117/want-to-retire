# ADR 0002: Point-in-time data access

Status: Accepted

## Context

Look-ahead can enter through the system as well as through the model. If a decision at time t can read anything published after t, even by accident, the forward test is no longer clean. Data also reaches the system some time after it happens: a candle closes, the exchange publishes it, and the collector receives it later still.

## Decision

- Every record stores two timestamps: `event_ts`, when the event happened at the source, and `available_ts`, when this system could first have known about it.
- A decision at time t receives a snapshot that contains only records with `available_ts ≤ t`.
- Decision modules have no other path to data. No query interface exists that could reach past the snapshot.

## Consequences

- Run-time look-ahead is prevented by the structure of the system itself.
- Collection latency has to be modelled explicitly. Decisions will run a fixed delay after each bar closes, and the delay will be set once collection latency has been measured.
- For history that was backfilled instead of collected live, `available_ts` cannot be observed and has to be estimated.
