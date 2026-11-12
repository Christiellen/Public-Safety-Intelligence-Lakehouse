-- Semantic layer for Superset: schema psi_bi.
--
-- Superset reads only these views, through a read-only role with no access to psi_gold. The views
-- keep the rules of the Gold layer: CVLI = victims (measure_type 'victims') of the events flagged
-- is_cvli in dim_event; measure types are never summed together; rates are annualized per 100,000
-- inhabitants. Rebuilt by load_gold.py on every load, in the same transaction as the tables.

CREATE SCHEMA psi_bi;

-- One row per municipality: indicators, hotspots, IVSC and spatial statistics.
CREATE VIEW psi_bi.municipality_profile AS
SELECT
    m.municipality_code,
    m.municipality_name,
    m.state_code,
    st.state_name,
    m.region_name,
    m.municipality_name || ' (' || m.state_code || ')' AS municipality_label,
    m.population,
    m.population_year::text AS population_year_label,
    m.gdp_per_capita_brl,
    m.economic_tier,
    m.idhm,
    m.fleet_total,
    m.cvli_victims,
    m.has_sinesp_records,
    m.cvli_rate_100k,
    m.cvli_z_score_state,
    m.cvli_z_score_national,
    m.is_state_hotspot,
    m.is_national_hotspot,
    m.ivsc_score,
    m.months_covered,
    m.window_start,
    m.window_end,
    COALESCE(s.lisa_cluster, 'No data') AS lisa_cluster,
    s.lisa_p_value,
    s.community_id,
    s.centroid_lat,
    s.centroid_lon,
    s.area_km2,
    s.neighbor_count,
    s.neighbor_avg_cvli_rate_100k,
    s.neighbor_national_hotspots
FROM psi_gold.municipality_analytics AS m
JOIN psi_gold.dim_state AS st USING (state_code)
LEFT JOIN psi_gold.municipality_spatial_metrics AS s USING (municipality_code);

-- Profile of a municipality as label/value rows (item_order keeps the display order), for a native
-- table chart. Values are formatted text. The profile chart groups by item with
-- HAVING COUNT(DISTINCT municipality_code) = 1, so it shows rows only when one municipality is selected.
CREATE VIEW psi_bi.municipality_profile_items AS
SELECT p.municipality_code, p.municipality_label, p.state_code, p.state_name, i.item_order, i.item, i.value
FROM psi_bi.municipality_profile AS p
CROSS JOIN LATERAL (VALUES
    (1, 'Population - ' || p.population_year_label, to_char(p.population, 'FM999,999,999')),
    (2, 'IVSC - 12-month window', COALESCE(to_char(p.ivsc_score, 'FM0.0000'), 'n/a')),
    (3, 'National Z-score', COALESCE(to_char(p.cvli_z_score_national, 'FM990.00'), 'n/a (under 20,000 inhabitants)')),
    (4, 'National hotspot', CASE WHEN p.is_national_hotspot THEN 'Yes' ELSE 'No' END),
    (5, 'LISA cluster', p.lisa_cluster || COALESCE(' (p = ' || to_char(p.lisa_p_value, 'FM0.000') || ')', '')),
    (6, 'IDHM (HDI) - 2010', COALESCE(to_char(p.idhm, 'FM0.000'), 'n/a'))
) AS i (item_order, item, value);

-- One row per state: lethal violence, vehicle crimes and drug seizures in the analysis window.
CREATE VIEW psi_bi.state_profile AS
SELECT
    state_code,
    state_name,
    region_code,
    region_name,
    population,
    fleet_total,
    cvli_victims,
    cvli_rate_100k,
    vehicle_crimes,
    vehicle_crime_rate_1k_fleet,
    drug_seizure_kg,
    months_covered,
    window_start,
    window_end
FROM psi_gold.state_analytics;

-- One row per state and month: CVLI victims, including victims without informed municipality
-- (published in the state facts only). population is the latest estimate of the state.
CREATE VIEW psi_bi.cvli_monthly_state AS
WITH cvli AS (
    SELECT f.state_code, f.date_id, SUM(f.value) AS cvli_victims
    FROM psi_gold.fact_state_occurrences AS f
    JOIN psi_gold.dim_event AS e USING (event_id)
    WHERE e.is_cvli AND f.measure_type = 'victims'
    GROUP BY f.state_code, f.date_id
)
SELECT
    c.state_code,
    st.state_name,
    st.region_name,
    d.date AS month,
    d.year,
    d.year_month,
    c.cvli_victims,
    sa.population,
    ROUND((c.cvli_victims * 12 * 100000.0 / NULLIF(sa.population, 0))::numeric, 2) AS cvli_rate_100k_annualized
FROM cvli AS c
JOIN psi_gold.dim_date AS d USING (date_id)
JOIN psi_gold.dim_state AS st USING (state_code)
LEFT JOIN psi_gold.state_analytics AS sa USING (state_code);

-- One row per area and month, on a complete grid (main dataset of the dashboard). Areas are the
-- municipalities with SINESP records, plus one "Not assigned" row per state with the CVLI victims
-- that the state facts have and the municipal facts do not (victims without informed municipality,
-- agencies that publish by state only). So any filter works with plain sums:
--   - a state, a region or Brazil: the sum equals the state facts (as in state_analytics);
--   - a municipality: only its own victims (the "Not assigned" rows drop out).
-- Months without records count as zero for a municipality with records, as in the Gold rates.
-- population is the latest estimate, repeated every month, so SUM(cvli_victims) * 12 * 100,000 /
-- SUM(population) is the annualized rate of any selection of areas and months. The "Not assigned"
-- row carries the population of the state's municipalities without records, so the population of a
-- state adds up to the population of all its municipalities.
CREATE VIEW psi_bi.cvli_monthly_municipality AS
WITH cvli_events AS (
    SELECT event_id FROM psi_gold.dim_event WHERE is_cvli
),
municipal AS (
    SELECT f.municipality_code, f.date_id, SUM(f.count)::double precision AS cvli_victims
    FROM psi_gold.fact_occurrences AS f
    JOIN cvli_events USING (event_id)
    WHERE f.measure_type = 'victims'
    GROUP BY f.municipality_code, f.date_id
),
reporting AS (
    SELECT DISTINCT municipality_code FROM municipal
),
municipal_rows AS (
    SELECT m.municipality_code, m.municipality_name, m.municipality_name || ' (' || m.state_code || ')' AS municipality_label,
           m.state_code, d.date, COALESCE(x.cvli_victims, 0) AS cvli_victims, m.population::double precision AS population
    FROM psi_gold.municipality_analytics AS m
    JOIN reporting USING (municipality_code)
    CROSS JOIN psi_gold.dim_date AS d
    LEFT JOIN municipal AS x ON x.municipality_code = m.municipality_code AND x.date_id = d.date_id
),
state_facts AS (
    SELECT f.state_code, d.date, SUM(f.value) AS cvli_victims
    FROM psi_gold.fact_state_occurrences AS f
    JOIN cvli_events USING (event_id)
    JOIN psi_gold.dim_date AS d USING (date_id)
    WHERE f.measure_type = 'victims'
    GROUP BY f.state_code, d.date
),
state_from_municipalities AS (
    SELECT state_code, date, SUM(cvli_victims) AS cvli_victims FROM municipal_rows GROUP BY state_code, date
),
silent_population AS (
    SELECT m.state_code, SUM(m.population)::double precision AS population
    FROM psi_gold.municipality_analytics AS m
    WHERE NOT EXISTS (SELECT 1 FROM reporting AS r WHERE r.municipality_code = m.municipality_code)
    GROUP BY m.state_code
),
unassigned_rows AS (
    SELECT NULL::text AS municipality_code, 'Not assigned' AS municipality_name,
           'Not assigned (' || st.state_code || ')' AS municipality_label, st.state_code, d.date,
           COALESCE(sf.cvli_victims, 0) - COALESCE(sm.cvli_victims, 0) AS cvli_victims,
           COALESCE(sp.population, 0) AS population
    FROM psi_gold.dim_state AS st
    CROSS JOIN psi_gold.dim_date AS d
    LEFT JOIN state_facts AS sf ON sf.state_code = st.state_code AND sf.date = d.date
    LEFT JOIN state_from_municipalities AS sm ON sm.state_code = st.state_code AND sm.date = d.date
    LEFT JOIN silent_population AS sp ON sp.state_code = st.state_code
),
area_rows AS (
    SELECT * FROM municipal_rows
    UNION ALL
    SELECT * FROM unassigned_rows
)
SELECT
    a.municipality_code,
    a.municipality_name,
    a.municipality_label,
    a.municipality_code IS NOT NULL AS is_municipality,
    a.state_code,
    st.state_name,
    st.region_name,
    a.date AS month,
    EXTRACT(YEAR FROM a.date)::integer AS year,
    EXTRACT(MONTH FROM a.date)::integer AS month_of_year,
    to_char(a.date, 'YYYY-MM') AS year_month,
    a.cvli_victims,
    p.cvli_victims AS cvli_victims_previous_year,
    a.population,
    CASE WHEN a.municipality_code IS NOT NULL THEN COALESCE(sm.lisa_cluster, 'No data') END AS lisa_cluster
FROM area_rows AS a
JOIN psi_gold.dim_state AS st USING (state_code)
LEFT JOIN psi_gold.municipality_spatial_metrics AS sm ON sm.municipality_code = a.municipality_code
LEFT JOIN area_rows AS p
    ON p.municipality_label = a.municipality_label AND p.date = (a.date - INTERVAL '12 months')::date;

-- One row: national KPIs of the analysis window, and the change against the previous window of the
-- same length. cvli_change_pct is null when the previous window is not complete.
CREATE VIEW psi_bi.cvli_kpis AS
WITH win AS (
    SELECT MIN(window_start) AS window_start, MAX(window_end) AS window_end, MAX(months_covered) AS months_covered
    FROM psi_gold.state_analytics
),
current_window AS (
    SELECT SUM(cvli_victims) AS cvli_victims, SUM(population) AS population
    FROM psi_gold.state_analytics
),
previous_window AS (
    SELECT SUM(f.value) AS cvli_victims, COUNT(DISTINCT f.date_id) AS months_covered
    FROM psi_gold.fact_state_occurrences AS f
    JOIN psi_gold.dim_event AS e USING (event_id)
    JOIN psi_gold.dim_date AS d USING (date_id)
    CROSS JOIN win
    WHERE e.is_cvli AND f.measure_type = 'victims'
      AND d.date < win.window_start
      AND d.date >= win.window_start - make_interval(months => win.months_covered)
)
SELECT
    win.window_start,
    win.window_end,
    win.months_covered,
    cur.cvli_victims,
    cur.population,
    ROUND((cur.cvli_victims * 12.0 / win.months_covered * 100000 / NULLIF(cur.population, 0))::numeric, 2)
        AS cvli_rate_100k,
    prev.cvli_victims AS previous_cvli_victims,
    prev.months_covered AS previous_months_covered,
    CASE WHEN prev.months_covered = win.months_covered AND prev.cvli_victims > 0
         THEN ROUND(((cur.cvli_victims / prev.cvli_victims - 1) * 100)::numeric, 1)
    END AS cvli_change_pct,
    (SELECT COUNT(*) FROM psi_gold.municipality_analytics WHERE is_national_hotspot) AS national_hotspots,
    (SELECT COUNT(*) FROM psi_gold.municipality_spatial_metrics WHERE lisa_cluster = 'High-High')
        AS high_high_municipalities
FROM win
CROSS JOIN current_window AS cur
CROSS JOIN previous_window AS prev;
