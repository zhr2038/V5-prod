# V5 Decision Receipt v2.2

This is a design and default-off recorder for exact decision-state capture. It is
not connected to the production V5 pipeline, is not deployed, and has no order
submission capability.

## Contract

The strict schema is `schemas/v5_decision_receipt_v22.schema.json`. A receipt binds
decision time, market-data cutoff, strategy and parameter identity, current
positions/cash, target weights, risk and gate results, order intents, execution
mode, final action, and error codes.

Receipts must never contain API keys, secrets, passphrases, private keys,
authorization headers, database passwords, or bearer tokens. The recorder rejects
sensitive keys and known credential markers recursively.

## Storage and failure behavior

`DecisionReceiptRecorder` is disabled unless `enabled=True` is passed explicitly.
An enabled recorder writes one canonical JSON file per receipt ID using an fsynced
temporary file and an atomic no-overwrite hard link. A byte-identical retry is
idempotent; a different payload for an existing receipt ID is an integrity error.

Production integration, if separately reviewed later, should call `try_record`
only as an observability side effect. It never raises. A write failure must not
block or delay a protective close, risk abort, or sell-only action. No integration
or deployment is part of Audit v2.2.

## Rollback

Because no production call site is changed, rollback is deletion of this isolated
branch or omission of the module from a future merge. Existing V5 runtime behavior
is unchanged.
