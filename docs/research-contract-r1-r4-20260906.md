# R1–R4 research contract repair, 2026-09-06

Natural startup exposed a cold-start omission in release `848361c6`: before the
first shared cutoff there is no original pipeline factor audit. Requiring it for
an otherwise flat quote-only observation failed with `factor_provenance_unobservable`.
The interrupted-processing guard stopped further writes: the attempt has zero
completed events and is preserved. `test_review_cold_start.py` reproduces this with
the real adapter and snapshot builder (no factor mocks). Flat pre-decision
observations now record only quotes; the actual shared decision still requires
complete real factor provenance. Failed attempt state is not cleared or replayed;
the corrected release starts another explicitly identified directory.

This release improves research credibility. It does not establish profitability,
replace the production strategy, alter live risk, or authorize live orders.

## Before-change evidence

The read-only qyun inspection at 2026-09-06 06:51:08 UTC found 688 observations
in `review-20260906-v1-a56d40f269d9`. Of the two actual C entry candidates,
zero had a valid reference at decision time. Later receipts exist in those hours.
These are candidate counts, not HTTP successes or SQL associations. This limited
window establishes no effective reference comparison, not absence of middle
platform value. qyun and qyun2 release manifests and dependency locks were also
verified before activation; page evidence is stored separately from source/logs.

The formal `test_review_boundary_regressions.py` was run against an isolated
archive of main `8e21c9e8dd3f4c1cef7e22919eebd72cf19f0f8d`: 15 failures and
2 passes. The archived red log uses a corrected complete synthetic snapshot
fixture (`data_errors=[]`); its failures reach the intended contract assertions.
The archive is unmodified source, not a reset of any running or research account.
Synthetic quotes and balances are test inputs, not historical or live PnL.

## Frozen v2 behavior

- R1: all A/B/C/D entries use the first observed quote at or after hour + 360s,
  no later than hour + 480s. The choice is fixed before this cohort begins.
  Missing/late references preserve the candidate. No group receives a private
  later decision or retroactive reference. Minute observations continue checking
  candidate exits. Execution still needs a strictly later observable quote.
  First opportunity, duplicate/reversed time, missed deadline, restart and late
  receipt tests cover this behavior. Gaps and quote expiry remain enforced.
- R2: a research entry floor of 10 USDT is separate from exchange minSz/lotSz.
  The public instrument response does not supply an additional notional floor;
  the model explicitly records that limitation instead of inventing a 10 USDT
  product specification. Exits use observed execution constraints only. Economic
  equity values all remaining units at bid net of modeled exit costs, while gross
  asset equity, immediately executable equity and restricted residual quantity,
  cost and estimated value remain separate. A falling price cannot turn a whole
  holding into a zero-value inactive residual. Actual completed exits may leave
  explicitly labeled residuals; their units and costs remain in the ledger.
- R3: schema, experiment, strategy, cost, signed worker commit and 24h horizon
  must all match `review_experiment_v2.json`. Missing metadata in old immutable
  receipts is not reconstructed. Latest first-received advice per symbol wins;
  unknown versions are recorded and cannot defer the frozen D group. Price/input/
  advice/publication refresh and presentation metadata may change within the same
  six bindings. Any different analysis source needs a new cohort. D studies only
  DEFER on an existing 24h candidate, never REVIEW_ENTRY signal creation.
- R4: each frozen requirement gets actual value, requirement, PASS/FAIL/
  INSUFFICIENT and reason. The original 30 days / 100 independent opportunities /
  10 entry days and profitability/interval/drawdown/stress/remove-best conditions
  remain. V2 predeclares D treatment exposure of at least 80% valid coverage,
  30 matched candidates and 10 non-overlapping matched changed opportunities.
  The funnel counts the union of actual C/D candidates by symbol/hour; D-only
  effects after account divergence are recorded, not lost or called matched pairs. Missing
  exposure cannot be reported as evidence of zero value. Passing every research
  row means manual research review only; live eligibility remains false.

A is the original hourly decision pipeline with shared quote execution, not a
complete recreation of production event execution. B/C/D may inspect minute exits.
No conclusion attributes all differences to entry direction. Cloud/API fixed
costs are still unspecified and project-level profit remains unverified.

## Isolation, UI and rollback

Keep v1 directories and SQLite history unchanged; stop scheduling the previous
review cohort and start v2 in a new directory. The independent original
participation ledger and its policy/runtime identity remain unchanged. New runner
code refuses the old v1 contract. There is no history replay or capital addition.

The existing strategy page adds a separate A/B/C/D report, candidate-time reference
funnel, three equity measures plus residuals and stops, frozen upstream versions,
report cutoff and per-condition acceptance. `reports/review_comparison/current.json`
explicitly selects one ledger and identity. API verifies pointer, manifest and
SQLite identity and shows stale/worker failure explicitly; it never scans for a
newer profitable directory or merges returns with other accounts.

Release via a new directory and verified manifest, preserving dependency locks,
runtime overrides and shared stores. Freeze/backup the old review under its lock.
Rollback changes code symlink, service verifier and research output pointer only;
never restore old account database contents over newer live orders or positions.
If reverting semantics, pause v2 and preserve it, then deliberately select the
compatible old code/cohort; do not run old code against the v2 ledger. Retain
activation backups and perform code-only rollback/rollforward while services and
execution locks are quiescent. Resume the existing schedules, then verify natural
worker, receipt and common decision cycles before claiming deployment complete.

Validation artifacts and actual release/CI identities are recorded in the local
delivery folder `E:\v5-prod\output\research-contract-r1-r4-20260906` and the
deployed release's `release-ops` directory. Local tests, complete CI and natural
observations are distinct evidence; none are a historical strategy profit test.
