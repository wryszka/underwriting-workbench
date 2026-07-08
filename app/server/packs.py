"""Decision audit pack — one PDF per decided submission, built from the as-at evidence.

Pure fpdf2 + stdlib (no server imports) so the batch notebook (12_decision_packs) and the
app can share it. The pack is the auditor's artefact: the dossier exactly as the underwriter
saw it, the decision, terms and subjectivities, the price build-up with IPT, and the
screening record with the external/internal separation made explicit.
"""
from fpdf import FPDF


def _m(v):
    try:
        return f"GBP {float(v):,.0f}"
    except (TypeError, ValueError):
        return "-"


class _Pack(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(30, 41, 59)
        self.cell(0, 5, "BRICKSURANCE SE - UNDERWRITING DECISION AUDIT PACK", align="L")
        self.set_font("Helvetica", "", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 5, "Synthetic demo - not a real insurer", align="R")
        self.ln(8)
        self.set_draw_color(37, 99, 235)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "", 7.5)
        self.set_text_color(148, 163, 184)
        self.cell(0, 5, f"Generated from gold_decision_audit - page {self.page_no()}", align="C")


def _w(pdf):
    return pdf.w - pdf.l_margin - pdf.r_margin


def _title(pdf, t):
    pdf.set_x(pdf.l_margin)
    pdf.set_font("Helvetica", "B", 10.5)
    pdf.set_text_color(37, 99, 235)
    pdf.cell(0, 7, t.upper())
    pdf.ln(8)


def _kv(pdf, k, v):
    pdf.set_x(pdf.l_margin)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(62, 5.5, str(k)[:44])
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(15, 23, 42)
    pdf.multi_cell(_w(pdf) - 62, 5.5, str(v if v not in (None, "") else "-")[:220])


def _bullets(pdf, items):
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(15, 23, 42)
    for it in (items or []):
        pdf.set_x(pdf.l_margin + 2)
        pdf.multi_cell(_w(pdf) - 4, 5.2, "- " + str(it)[:400])
    if not items:
        pdf.set_x(pdf.l_margin + 2)
        pdf.multi_cell(_w(pdf) - 4, 5.2, "- none")
    pdf.ln(1)


def build_pack(audit: dict, evidence: dict) -> bytes:
    """audit = the gold_decision_audit row (dict); evidence = the as-at panels dict."""
    ev = evidence or {}
    es, prc = ev.get("dossier") or {}, ev.get("price") or {}
    app_, auth = ev.get("appetite") or {}, ev.get("authority") or {}
    acc, scr = ev.get("accumulation") or {}, ev.get("screening") or {}
    rec = ev.get("recommendation") or {}

    pdf = _Pack()
    pdf.set_auto_page_break(True, margin=16)
    pdf.add_page()

    _title(pdf, f"Decision {audit.get('decision_id', '')} - {audit.get('submission_public_id', '')}")
    _kv(pdf, "Company", es.get("company_name"))
    _kv(pdf, "Trade / segment", f"{es.get('trade_group', '-')} / {es.get('segment', '-')}")
    _kv(pdf, "Broker / channel", f"{es.get('broker_id', '-')} / {es.get('channel', '-')}")
    _kv(pdf, "Decision", str(audit.get("action", "")).upper()
        + (f" -> {audit.get('refer_to_grade')}" if audit.get("refer_to_grade") else ""))
    _kv(pdf, "Decided by / via / at",
        f"{audit.get('decided_by', '-')} / {audit.get('decided_via', '-')} / {audit.get('decision_ts', '-')}")
    _kv(pdf, "Straight-through", "yes" if str(audit.get("straight_through")).lower() == "true" else "no")
    pdf.ln(2)

    _title(pdf, "The checks, as the underwriter saw them")
    _kv(pdf, "Appetite", f"{app_.get('appetite_status', '-')} (guide {app_.get('guide_section', '-')})")
    _kv(pdf, "Authority required", auth.get("required_grade"))
    _kv(pdf, "Accumulation (worst district)",
        f"{acc.get('worst_district', '-')} at {acc.get('worst_post_util_pct', '-')}% post-bind ({acc.get('worst_status', '-')})")
    _kv(pdf, "Screening", str(scr.get("status", "-")).replace("_", " "))
    _kv(pdf, "Fair presentation",
        f"turnover stated {_m(es.get('turnover_stated'))} vs filed {_m(es.get('filed_turnover'))}"
        if es.get("filed_turnover") else "no filed-accounts mismatch identified")
    pdf.ln(2)

    _title(pdf, "Technical price")
    _kv(pdf, "Base premium", _m(prc.get("base_premium")))
    _kv(pdf, "+ theft/malicious damage loading", _m(prc.get("crime_theft_loading")))
    _kv(pdf, "+ flood loading", _m(prc.get("flood_loading")))
    _kv(pdf, "+ claims experience loading", _m(prc.get("claims_experience_loading")))
    _kv(pdf, "Technical premium", _m(prc.get("technical_premium")))
    _kv(pdf, "IPT at 12%", _m(prc.get("ipt_amount")))
    _kv(pdf, "Total payable", _m(prc.get("total_inc_ipt")))
    _kv(pdf, "Broker target / adequacy",
        f"{_m(prc.get('target_premium'))} / {prc.get('adequacy_pct', '-')}%" if prc.get("target_premium") else "priced to guide")
    pdf.ln(2)

    _title(pdf, "Reasons")
    _bullets(pdf, audit.get("reasons") or rec.get("reasons"))
    _title(pdf, "Terms")
    _bullets(pdf, audit.get("terms") or rec.get("terms"))
    _title(pdf, "Subjectivities")
    _bullets(pdf, audit.get("subjectivities") or rec.get("subjectivities"))

    if audit.get("decline_code_external") or audit.get("external_reason"):
        _title(pdf, "External communication (broker-facing)")
        _kv(pdf, "Coded reason", audit.get("decline_code_external"))
        _kv(pdf, "External wording", audit.get("external_reason"))

    _title(pdf, "Internal record (never broker-facing)")
    _bullets(pdf, audit.get("internal_notes") or rec.get("internal_notes"))
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(100, 116, 139)
    pdf.set_x(pdf.l_margin)
    pdf.multi_cell(_w(pdf), 4.6,
                   "Reproducibility: this pack is rendered from the decision_evidence snapshot stored at "
                   "decision time in gold_decision_audit - the panels are never recomputed for the auditor.")
    return bytes(pdf.output())
