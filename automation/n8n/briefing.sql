-- Weekly briefing data: one row, one JSON column, read by the n8n workflow through the
-- Databricks SQL Statement Execution API. Every number in the e-mail comes from here; the LLM
-- only writes the text around them.
SELECT to_json(named_struct(
  'window_start', (SELECT CAST(min(window_start) AS STRING) FROM workspace.psi_gold.state_analytics),
  'window_end',   (SELECT CAST(max(window_end) AS STRING) FROM workspace.psi_gold.state_analytics),
  'national', (
    SELECT named_struct(
      'cvli_victims', CAST(sum(cvli_victims) AS BIGINT),
      'population', CAST(sum(population) AS BIGINT),
      'cvli_rate_100k', round(sum(cvli_victims) / sum(population) * 100000 * 12 / max(months_covered), 1),
      'vehicle_crimes', CAST(sum(vehicle_crimes) AS BIGINT),
      'drug_seizure_kg', round(sum(drug_seizure_kg), 0))
    FROM workspace.psi_gold.state_analytics),
  'highest_state_rates', (
    SELECT array_sort(collect_list(named_struct(
      'rank', rank, 'state', state_code, 'region', region_name,
      'cvli_rate_100k', round(cvli_rate_100k, 1), 'cvli_victims', CAST(cvli_victims AS BIGINT))))
    FROM (SELECT *, row_number() OVER (ORDER BY cvli_rate_100k DESC, state_code) AS rank
          FROM workspace.psi_gold.state_analytics WHERE cvli_rate_100k IS NOT NULL)
    WHERE rank <= 5),
  'lowest_state_rates', (
    SELECT array_sort(collect_list(named_struct(
      'rank', rank, 'state', state_code, 'region', region_name,
      'cvli_rate_100k', round(cvli_rate_100k, 1), 'cvli_victims', CAST(cvli_victims AS BIGINT))))
    FROM (SELECT *, row_number() OVER (ORDER BY cvli_rate_100k ASC, state_code) AS rank
          FROM workspace.psi_gold.state_analytics WHERE cvli_rate_100k IS NOT NULL)
    WHERE rank <= 5),
  'largest_city_hotspots', (
    SELECT array_sort(collect_list(named_struct(
      'rank', rank, 'municipality', municipality_name, 'state', state_code, 'population', population,
      'cvli_rate_100k', round(cvli_rate_100k, 1), 'lisa_cluster', lisa_cluster)))
    FROM (SELECT a.*, s.lisa_cluster,
                 row_number() OVER (ORDER BY a.cvli_rate_100k DESC, a.municipality_code) AS rank
          FROM workspace.psi_gold.municipality_analytics a
          LEFT JOIN workspace.psi_gold.municipality_spatial_metrics s USING (municipality_code)
          WHERE a.is_national_hotspot AND a.population >= 100000)
    WHERE rank <= 10),
  'national_hotspots', (SELECT count(*) FROM workspace.psi_gold.municipality_analytics WHERE is_national_hotspot),
  'high_high_municipalities', (
    SELECT count(*) FROM workspace.psi_gold.municipality_spatial_metrics WHERE lisa_cluster = 'High-High'),
  'data_changes_last_7_days', (
    SELECT array_sort(collect_list(named_struct('dataset', dataset, 'keys', keys)))
    FROM (SELECT dataset, array_join(array_sort(collect_set(key)), ', ') AS keys
          FROM workspace.psi_ops.change_log
          WHERE changed_at >= current_timestamp() - INTERVAL 7 DAYS
          GROUP BY dataset)),
  'data_quality', named_struct(
    'unmatched_municipality_names', (SELECT count(*) FROM workspace.psi_silver.qa_unmatched_municipalities),
    'fuzzy_matches_to_review', (SELECT count(*) FROM workspace.psi_silver.qa_fuzzy_matches))
)) AS briefing
