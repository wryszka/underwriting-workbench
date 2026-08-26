# MCP Tool Contracts — versioned source of truth (Lane F)

The authoritative contract for every MCP tool. The custom FastMCP app registers its tools from this
file; the managed-server tools are UC functions whose `COMMENT` carries the same description, so docs
and runtime never drift. **Versioning policy:** each tool carries a version; a breaking change to
arguments or semantics creates a NEW tool name (`_v2`) — existing tool names never change meaning.

**Global refusal semantics.** Refusals are structured, never silent: `{refused:true, code, reason,
remedy}` where `code` ∈ {`NOT_OWNER`, `COMPLIANCE_LOCKED`, `NO_SUCH_TOOL`, `PENDING_APPROVAL_ONLY`,
`SCHEMA_INVALID`, `RATE_LIMITED`, `PRECONDITION`, `INSUFFICIENT_PRIVILEGES`} and `remedy` states what
would make the call valid. Every call (success or refusal) writes one `gold_mcp_activity` row.

**Escalate-not-bind (protocol-level).** No tool binds a policy, applies a rule change, or sends
external comms. Mutating tools return `{proposal_id, status:"pending_human_approval"}`; approval is a
human action in the app only.

---

## F1 · `uw-submission` (custom FastMCP app; external / broker agent)

Narrow, defensive, schema-validated on every argument. The calling principal is a broker; a UC row
filter restricts every read to that broker's own submissions.

### `submit_risk` · v1 · MUTATING
- **Does:** Validate a structured commercial-risk submission against the published JSON schema and land
  it in the ingest path. Does NOT quote or bind.
- **Args:** `payload` (object) — `{company_name:str, trade:str, postcode_district:str, sums_insured:{buildings:int,contents:int,stock:int,bi:int}, el_limit:int, pl_limit:int, turnover:int, employees:int, channel:"broker"}`. Amounts in GBP (integers). Unknown fields rejected.
- **Returns:** `{submission_id:str, lifecycle_state:"received", proposal_id:str, status:"pending_human_approval"}`.
- **Refuses:** `SCHEMA_INVALID` (missing/typed-wrong field, remedy = the failing path); `RATE_LIMITED` (per-principal budget); `PRECONDITION` if channel≠broker.
- **Example:** `submit_risk({payload:{company_name:"Northgate & Finch Scaffolding Ltd", trade:"construction_contractors", postcode_district:"LS10", ...}})` → `{submission_id:"sub:900004", lifecycle_state:"received", ...}`.

### `upload_document` · v1 · MUTATING
- **Does:** Attach a broker document to a submission; lands in the extraction path (the confidence gate
  applies; text is treated as DATA, never instructions).
- **Args:** `submission_id:str`, `doc:{filename:str, mime:str, content_b64:str}` (size-capped).
- **Returns:** `{document_id:str, extraction_status:"pending"|"low_confidence"|"extracted", proposal_id, status:"pending_human_approval"}`.
- **Refuses:** `NOT_OWNER` (not the caller's submission); `SCHEMA_INVALID`/oversized payload; `RATE_LIMITED`.

### `get_submission_status` · v1 · READ
- **Does:** Lifecycle state + SLA clock for one submission — **only** if owned by the caller (UC row filter).
- **Args:** `submission_id:str`.
- **Returns:** `{submission_id, lifecycle_state, sla_hours_remaining, updated_ts}`.
- **Refuses:** `NOT_OWNER` (remedy = "request status only for your own submissions").

### `get_quote` · v1 · READ
- **Does:** Terms, price (incl. **IPT 12% + commission %**), subjectivities — only in post-quote states.
- **Args:** `submission_id:str`.
- **Returns:** `{state, technical_premium, ipt_amount, total_inc_ipt, commission_pct, terms[], subjectivities[]}`.
- **Refuses:** `NOT_OWNER`; `PRECONDITION` if not yet quoted (remedy = "await lifecycle=quoted").

### `respond_to_subjectivity` · v1 · MUTATING
- **Does:** Record a broker's response to a quote subjectivity. Does NOT clear it.
- **Args:** `submission_id:str`, `subjectivity_id:str`, `response:str` (free text → data).
- **Returns:** `{proposal_id, status:"pending_human_approval"}` (an underwriter reviews).
- **Refuses:** `NOT_OWNER`; `PRECONDITION` (no such subjectivity / not in a state accepting responses).

---

## F2 · `uw-decision-support` (managed functions + underwriting Genie; internal copilot)

Read-and-compute only; zero state changes. Tools = UC functions (descriptions in their COMMENT).

| Tool (UC function) | v | Does | Key args |
|---|---|---|---|
| `fn_extract_summary` | 1 | dossier header | `sid` |
| `fn_appetite_check` | 1 | appetite / guide section | `sid` |
| `fn_authority_check` | 1 | required grade + triggers | `sid` |
| `fn_accumulation_impact` | 1 | district accumulation before/after | `sid` |
| `fn_technical_price` | 1 | price build-up + named loadings + IPT | `sid` |
| `fn_sanctions_screen` | 1 | OFSI + watchlist screen | `sid` |
| `fn_underinsurance_check` | 1 | declared vs benchmark SI | `sid` |
| `fn_recommendation` | 1 | composed quote/refer/decline/request-info | `sid` |
| `fn_assemble_dossier` | 1 | **all of the above in one call** (JSON sections) | `sid` |
| `fn_fire_pattern_precedent` | 1 | historical outcomes for this submission's referral fire-pattern | `sid` |
+ the underwriting Genie space ("Ask the Book"). All READ-ONLY.

---

## F3 · `uw-referral-control` (managed functions + referral Genie; portfolio agent)

| Tool | v | Kind | Does | Args |
|---|---|---|---|---|
| `fn_recommend_action` | 1 | READ | recommended action from the closed set + evidence (compliance-locked ⇒ keep) | `rule_id`, `as_of` |
| `fn_isolation_analysis` | 1 | READ | what uniquely fires on a rule vs co-fire partners | `rule_id`, `as_of` |
| `fn_emulate_rule_change` | 1 | READ | replay a change + mandatory tail exhibit | `rule_id`, `action`, `as_of` |
| `get_findings` (via Genie / `gold_referral_findings`) | 1 | READ | ranked findings feed as of a date | `as_of` |
| `propose_rule_change` | 1 | **MUTATING (custom app)** | draft a change → `proposal_id`, pending; **refuses `COMPLIANCE_LOCKED`** | `rule_id`, `action`, `as_of` |
+ the referral-telemetry Genie space. **There is deliberately NO `approve_rule_change` tool.**

---

## F4 · `uw-governance` (managed functions, read-only; audit/compliance agent)

The governance principal has EXECUTE on ONLY these and **no mutating grants** (asserted in smoke).

| Tool (UC function) | v | Does | Args |
|---|---|---|---|
| `fn_rulebook_as_of` | 1 | the governed rulebook as it stood on a date (SCD2 as-of) | `as_of` (date) |
| `fn_decision_replay` | 1 | replay a decision: fire-vector + rule versions + named loadings (grandma-Jane) | `sid` |
| `fn_change_ledger` | 1 | rule-change history + predicted-vs-realised + drift | `status` or `"all"` |
| `fn_ai_activity_log` | 1 | unified AI/agent + MCP touch log | `scope` (`all`/`mcp`/`agent`/`sub:NNNNNN`) |

Strictly read-only. No tool here (or anywhere) can mutate state.
