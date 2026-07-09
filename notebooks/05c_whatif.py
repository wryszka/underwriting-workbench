# Databricks notebook source
# MAGIC %md
# MAGIC # 05c · What-if functions — Try a submission
# MAGIC
# MAGIC Parameter-driven twins of the crux for the live what-if form: price a hypothetical risk
# MAGIC and test a hypothetical accumulation without a submission row existing.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "underwriting_workbench")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_price_whatif(
    p_trade STRING, p_district STRING, p_buildings BIGINT, p_plant BIGINT, p_contents BIGINT,
    p_stock BIGINT, p_bi BIGINT, p_employees INT, p_turnover BIGINT, p_target BIGINT)
RETURNS STRUCT<property_component DOUBLE, bi_component DOUBLE, el_component DOUBLE, pl_component DOUBLE,
               base_premium DOUBLE, crime_theft_loading DOUBLE, flood_loading DOUBLE,
               technical_premium DOUBLE, ipt_amount DOUBLE, total_inc_ipt DOUBLE,
               adequacy_pct DOUBLE, flood_band STRING, hazard_grade INT, appetite_status STRING>
COMMENT 'What-if technical price for a hypothetical risk (Try a submission): rate-guide components + crime/flood loadings for the chosen district, IPT at 12 percent, adequacy vs an optional target. Inputs: trade_group, postcode_district, sums insured, employees, turnover, target premium (nullable).'
RETURN SELECT named_struct(
  'property_component', round(x.prop_c, 0), 'bi_component', round(x.bi_c, 0),
  'el_component', round(x.el_c, 0), 'pl_component', round(x.pl_c, 0),
  'base_premium', round(x.base, 0), 'crime_theft_loading', round(x.crime_l, 0),
  'flood_loading', round(x.flood_l, 0),
  'technical_premium', round(x.base + x.crime_l + x.flood_l, 0),
  'ipt_amount', round((x.base + x.crime_l + x.flood_l) * 0.12, 0),
  'total_inc_ipt', round((x.base + x.crime_l + x.flood_l) * 1.12, 0),
  'adequacy_pct', CASE WHEN p_target IS NOT NULL
                       THEN round(p_target / (x.base + x.crime_l + x.flood_l) * 100, 1) END,
  'flood_band', x.fb, 'hazard_grade', x.hz, 'appetite_status', x.app)
FROM (
  SELECT (p_buildings + p_plant + p_contents + p_stock) * any_value(r.property_rate_permille) / 1000 AS prop_c,
         p_bi * any_value(r.bi_rate_permille) / 1000 AS bi_c,
         p_employees * any_value(r.el_rate_per_employee) AS el_c,
         p_turnover / 1000 * any_value(r.pl_rate_per_1k_turnover) AS pl_c,
         greatest(any_value(r.min_premium),
                  (p_buildings + p_plant + p_contents + p_stock) * any_value(r.property_rate_permille) / 1000
                  + p_bi * any_value(r.bi_rate_permille) / 1000
                  + p_employees * any_value(r.el_rate_per_employee)
                  + p_turnover / 1000 * any_value(r.pl_rate_per_1k_turnover)) AS base,
         ((p_contents + p_stock) * any_value(r.property_rate_permille) / 1000) * (least(coalesce(any_value(c.effective_count), 0), 150) / 150.0) * 0.35 AS crime_l,
         CASE WHEN any_value(f.flood_band) = 'High'
              THEN (p_buildings + p_plant + p_contents + p_stock) * any_value(r.property_rate_permille) / 1000 * 0.25
              ELSE 0 END AS flood_l,
         coalesce(any_value(f.flood_band), 'Low') AS fb,
         any_value(a.hazard_grade) AS hz, any_value(a.appetite_status) AS app
  FROM {fqn}.ref_rate_guide r
  JOIN {fqn}.ref_appetite a USING (trade_group)
  LEFT JOIN {fqn}.ref_crime_open c ON c.postcode_district = p_district
  LEFT JOIN {fqn}.ref_flood_open f ON f.postcode_district = p_district
  WHERE r.trade_group = p_trade
) x
""")
print("created: fn_price_whatif")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_accumulation_whatif(p_district STRING, p_marginal_si BIGINT)
RETURNS STRUCT<postcode_district STRING, in_force_si BIGINT, capacity BIGINT, current_util_pct DOUBLE,
               post_util_pct DOUBLE, headroom_gbp BIGINT, status STRING, flood_band STRING>
COMMENT 'What-if accumulation: add a hypothetical marginal property SI to a district and see utilisation vs capacity before/after (80 percent = referral line, 100 percent = breach). Inputs: postcode_district, marginal property SI in GBP.'
RETURN SELECT named_struct(
  'postcode_district', any_value(postcode_district), 'in_force_si', any_value(in_force_property_si),
  'capacity', any_value(property_capacity_gbp), 'current_util_pct', any_value(utilisation_pct),
  'post_util_pct', round((any_value(in_force_property_si) + p_marginal_si) / any_value(property_capacity_gbp) * 100, 1),
  'headroom_gbp', any_value(property_capacity_gbp) - any_value(in_force_property_si) - p_marginal_si,
  'status', CASE WHEN (any_value(in_force_property_si) + p_marginal_si) / any_value(property_capacity_gbp) >= 1.0 THEN 'breach'
                 WHEN (any_value(in_force_property_si) + p_marginal_si) / any_value(property_capacity_gbp) >= 0.8 THEN 'referral'
                 ELSE 'ok' END,
  'flood_band', any_value(flood_band))
FROM {fqn}.gold_accumulation WHERE postcode_district = p_district
""")
print("created: fn_accumulation_whatif")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_mta_check(p_mta_id STRING)
RETURNS STRUCT<mta_id STRING, policy_number STRING, mta_type STRING, description STRING,
               delta_property_si BIGINT, postcode_district STRING,
               district_util_before_pct DOUBLE, district_util_after_pct DOUBLE, accumulation_status STRING,
               pro_rata_additional_premium DOUBLE, days_remaining INT, required_grade STRING,
               recommendation STRING, reasons ARRAY<STRING>>
COMMENT 'Mid-term adjustment (endorsement) delta checks: the ACCUMULATION DELTA in the affected district before/after, pro-rata additional premium for the remaining policy period, and the authority the delta premium requires. Recommendation approve/refer - a human endorses. Input: mta_id like mta:900010.'
RETURN SELECT named_struct(
  'mta_id', x.mta_id, 'policy_number', x.policy_number, 'mta_type', x.mta_type,
  'description', x.description, 'delta_property_si', x.delta, 'postcode_district', x.district,
  'district_util_before_pct', x.util_before,
  'district_util_after_pct', round((x.in_force + x.delta) / x.capacity * 100, 1),
  'accumulation_status', CASE WHEN (x.in_force + x.delta) / x.capacity >= 1.0 THEN 'breach'
                              WHEN (x.in_force + x.delta) / x.capacity >= 0.8 THEN 'referral' ELSE 'ok' END,
  'pro_rata_additional_premium', round(x.delta * x.rate / 1000 * x.days_rem / 365, 0),
  'days_remaining', x.days_rem,
  'required_grade', CASE WHEN x.delta * x.rate / 1000 * x.days_rem / 365 > 50000 THEN 'senior_underwriter'
                         WHEN x.delta * x.rate / 1000 * x.days_rem / 365 > 5000 THEN 'underwriter'
                         ELSE 'assistant_underwriter' END,
  'recommendation', CASE WHEN (x.in_force + x.delta) / x.capacity >= 0.8 THEN 'refer' ELSE 'approve' END,
  'reasons', filter(array(
     CASE WHEN (x.in_force + x.delta) / x.capacity >= 0.8
          THEN concat('Endorsement takes ', x.district, ' to ',
                      round((x.in_force + x.delta) / x.capacity * 100, 1), ' percent of capacity - the DELTA needs a referral even though the policy is in force') END,
     CASE WHEN (x.in_force + x.delta) / x.capacity < 0.8 THEN 'Delta within district capacity and authority - endorse and collect the additional premium' END
   ), r -> r IS NOT NULL))
FROM (
  SELECT any_value(m.mta_id) AS mta_id, any_value(m.policy_number) AS policy_number,
         any_value(m.mta_type) AS mta_type, any_value(m.description) AS description,
         any_value(m.delta_buildings_si + m.delta_contents_si) AS delta,
         any_value(m.postcode_district) AS district,
         any_value(a.utilisation_pct) AS util_before,
         any_value(a.in_force_property_si) AS in_force, any_value(a.property_capacity_gbp) AS capacity,
         any_value(coalesce(r.property_rate_permille, 3.5)) AS rate,
         any_value(greatest(datediff(to_date(p.expiry_date), current_date()), 0)) AS days_rem
  FROM {fqn}.landing_mta_feed m
  JOIN {fqn}.gold_accumulation a ON a.postcode_district = m.postcode_district
  JOIN {fqn}.landing_pas_policies p ON p.policy_number = m.policy_number
  LEFT JOIN {fqn}.ref_rate_guide r ON r.trade_group = p.trade_group
  WHERE m.mta_id = p_mta_id) x
""")
print("created: fn_mta_check")
print("✅ 05c complete")
