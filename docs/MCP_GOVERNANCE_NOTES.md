# MCP Governance Notes — identity, principals, grants (Lane F, F-P3 as-built)

Records exactly what was applied on `fevm-lr-dev-aws-us` for the MCP tool surface, and the one honest
degradation vs the ideal OBO end-to-end.

## Service principals (created — SP creation is permitted on this workspace)
- `uw_broker_agent` — appId `c907c94f-461a-4f52-9745-070a17408365` (external broker agent).
- `uw_governance_agent` — appId `4c369db2-ffee-4147-9b98-cb0bb1432d72` (audit/compliance agent).
- `uw-mcp` custom app SP — `66f27079-8d7b-40e8-882a-ddadb14b4951` (runs the FastMCP server).

## Grants applied
- **Governance SP (F4) — read-only, cannot mutate.** EXECUTE on `fn_rulebook_as_of`,
  `fn_decision_replay`, `fn_change_ledger`, `fn_ai_activity_log` (+ the read crux/Lane E functions those
  transitively call, since UC SQL functions run with INVOKER rights) and SELECT on the read tables they
  touch. **No MODIFY / INSERT / CREATE anywhere** → an attempted write fails (F-P6 smoke asserts).
  *Refinement:* making the F4 functions `SQL SECURITY DEFINER` would let the governance SP hold ONLY the
  4 EXECUTE grants (no transitive read grants); left as a follow-up to avoid recreating the functions.
- **Broker SP (F1).** SELECT on the row-filter view `mcp_broker_submissions` + SELECT on
  `ref_mcp_broker_identity` + EXECUTE on the F1-relevant read crux functions. No write grants.
- **App SP (uw-mcp).** `USE CATALOG` + (`USE SCHEMA`, `SELECT`, `MODIFY`, `EXECUTE`) on the schema +
  `CAN_USE` on the warehouse — the minimum for the write-path tools (INSERT into landing_submissions_feed
  / gold_mcp_proposals / gold_mcp_activity / gold_rule_changes, EXECUTE fn_emulate_rule_change).

## Broker row-filtering (UC-level)
- `ref_mcp_broker_identity(principal, broker_id, label)` maps a principal (email or SP appId) → broker_id.
- View `mcp_broker_submissions` = `SELECT * FROM landing_submissions_feed WHERE broker_id IN (SELECT
  broker_id FROM ref_mcp_broker_identity WHERE principal = current_user()) OR
  is_account_group_member('underwriters')`. A broker connecting **directly as its SP** (e.g. via managed
  MCP / SQL) sees ONLY its own submissions — enforced in Unity Catalog, not app code.

## The one honest degradation (OBO through the custom app)
The custom FastMCP app authenticates the caller via Databricks Apps and reads the forwarded identity
(`X-Forwarded-Email`), maps it to a broker_id via `ref_mcp_broker_identity`, and **enforces ownership
per-call** (every broker tool filters by that broker_id — verified: `get_submission_status` on another
broker's submission returns `NOT_OWNER`). It does NOT yet forward the broker's identity to Unity Catalog
for the tool's own SQL — those queries run as the app SP. So for calls **through the app**, ownership is
app-enforced against the authenticated caller identity + backed by the identical UC view logic; for
**direct principal** access the UC row filter is the enforcement. Production would enable Databricks
Apps OBO so the app forwards the broker identity and the same `mcp_broker_submissions` row filter applies
transparently end-to-end — collapsing the two into one UC-enforced path. The behaviour (a broker only
ever sees its own submissions) is identical today; only the enforcement layer differs.
