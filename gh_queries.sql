================================================================================
Ghana network analysis — source queries
Table: cell_tower_info_v2:mobile   Window: 2026-08-31 .. 2026-09-27 (28 days)
Generated 2026-09-29. Each query below produced the matching CSV in this folder.

Daiquery links (MUTABLE - the SQL below is the authoritative copy):
  diurnal cube    https://fburl.com/daiquery/fq8xnefa
  geo/quadkey     https://fburl.com/daiquery/96vxiayd   (blocked for the agent by DSS4)
  quadkey small   https://fburl.com/daiquery/nuxaj8g0
  TAC centroids   https://fburl.com/daiquery/shzlzzj3
  volume counts   https://fburl.com/daiquery/6yxxwdty

Extraction method: run per-day via `meta hive.table query --limit=100000 -o csv`,
then concatenate. A single 28-day run exceeds the 120s meta-CLI dispatch ceiling.
================================================================================


================================================================================
-- QUERY: cube_tac_v2.sql   ->  gh_tac_28d_v2.csv
================================================================================

-- ============================================================================
-- Ghana TAC cube: ds x operator x generation x tac
-- table: cell_tower_info_v2:mobile
-- ----------------------------------------------------------------------------
-- Geography WITHOUT lat/long. `tac` (tracking area code) comes from
-- registered_cell_info, which carries no UDP/DSS4 restriction. Ghana has ~1,787
-- distinct TACs -- FINER than the z=10 quadkey grid (294 tiles) -- so this is
-- the better geographic key unless you specifically need a projected grid.
-- Sentinels guarded (2147483647 signal / -1 packet counters); all measures
-- weighted by `weight`; loss carries raw numerators so it re-aggregates.
-- ============================================================================
SELECT
  ds,
  CASE WHEN op_mcc='620' AND op_mnc=1 THEN 'MTN GH'
       WHEN op_mcc='620' AND op_mnc=2 THEN 'Telecel GH'
       WHEN op_mcc='620' AND op_mnc=3 THEN 'AirtelTigo GH'
       WHEN op_mcc='620'              THEN 'other GH'
       WHEN op_mcc IS NULL            THEN 'unknown/no-cell'
       ELSE 'roaming/foreign' END AS operator,
  op_mcc, op_mnc, generation, tac,
  -- ================= VOLUME / FOOTPRINT =================
  count(*)                                              AS raw_rows,
  sum(w)                                                AS est_sessions,
  approx_distinct(ci)                                   AS cells,
  approx_distinct(tac)                                  AS tacs,
  approx_distinct(arfcn)                                AS arfcns,
  approx_distinct(device_model)                         AS device_models,
  -- ================= LATENCY =================
  approx_percentile(rtt, w, 0.10)                       AS p10_rtt,
  approx_percentile(rtt, w, 0.25)                       AS p25_rtt,
  approx_percentile(rtt, w, 0.50)                       AS p50_rtt,
  approx_percentile(rtt, w, 0.75)                       AS p75_rtt,
  approx_percentile(rtt, w, 0.90)                       AS p90_rtt,
  approx_percentile(rtt, w, 0.95)                       AS p95_rtt,
  approx_percentile(rtt, w, 0.99)                       AS p99_rtt,
  approx_percentile(ttfb, w, 0.50)                      AS p50_ttfb,
  approx_percentile(ttfb, w, 0.90)                      AS p90_ttfb,
  approx_percentile(ttfb, w, 0.95)                      AS p95_ttfb,
  approx_percentile(ttlb, w, 0.50)                      AS p50_ttlb,
  approx_percentile(ttlb, w, 0.90)                      AS p90_ttlb,
  approx_percentile(upstream_latency, w, 0.50)          AS p50_upstream,
  approx_percentile(upstream_latency, w, 0.90)          AS p90_upstream,
  approx_percentile(upload_latency, w, 0.50)            AS p50_upload,
  approx_percentile(upload_latency, w, 0.90)            AS p90_upload,
  approx_percentile(rtt_variance, w, 0.50)              AS p50_rtt_var,
  approx_percentile(rtt_variance, w, 0.90)              AS p90_rtt_var,
  -- ================= LOSS (rates + raw numerators for re-aggregation) ======
  1e2*sum(tpl *w)/nullif(sum(tp *w),0)                  AS loss_pct,
  1e2*sum(stpl*w)/nullif(sum(stp*w),0)                  AS srv_loss_pct,
  sum(tp  *w)                                           AS pkts_w,
  sum(tpl *w)                                           AS pkts_lost_w,
  sum(stp *w)                                           AS srv_pkts_w,
  sum(stpl*w)                                           AS srv_pkts_lost_w,
  1e2*sum(IF(tpl > 0, w, 0))/nullif(sum(IF(tp IS NOT NULL, w, 0)),0) AS pct_sessions_with_loss,
  -- ================= OUTCOMES =================
  1e2*sum(IF(request_status='done', w, 0))/nullif(sum(w),0)        AS ok_pct,
  1e2*sum(IF(status_code >= 500, w, 0))/nullif(sum(w),0)           AS http5xx_pct,
  1e2*sum(IF(status_code BETWEEN 400 AND 499, w, 0))/nullif(sum(w),0) AS http4xx_pct,
  1e2*sum(IF(tower_changed, w, 0))/nullif(sum(w),0)                AS tower_changed_pct,
  approx_percentile(congestion_state, w, 0.50)          AS p50_congestion,
  1e2*sum(IF(congestion_state > 1, w, 0))/nullif(sum(w),0)         AS pct_congested,
  -- ================= BYTES / THROUGHPUT =================
  approx_percentile(resp_bytes, w, 0.50)                AS p50_resp_bytes,
  approx_percentile(resp_bytes, w, 0.90)                AS p90_resp_bytes,
  approx_percentile(req_bytes,  w, 0.50)                AS p50_req_bytes,
  approx_percentile(resp_hdr,   w, 0.50)                AS p50_resp_hdr_bytes,
  approx_percentile(req_hdr,    w, 0.50)                AS p50_req_hdr_bytes,
  sum(resp_bytes * w)                                   AS resp_bytes_w,
  sum(req_bytes  * w)                                   AS req_bytes_w,
  -- ================= SIGNAL (RAT-specific; n_* are denominators) ==========
  approx_percentile(signal_dbm, w, 0.10)                AS p10_signal_dbm,
  approx_percentile(signal_dbm, w, 0.50)                AS p50_signal_dbm,
  approx_percentile(signal_dbm, w, 0.90)                AS p90_signal_dbm,
  count(signal_dbm)                                     AS n_signal_dbm,
  approx_percentile(rsrp_4g, w, 0.10)                   AS p10_rsrp_4g,
  approx_percentile(rsrp_4g, w, 0.50)                   AS p50_rsrp_4g,
  approx_percentile(rsrp_4g, w, 0.90)                   AS p90_rsrp_4g,
  count(rsrp_4g)                                        AS n_rsrp_4g,
  approx_percentile(rsrq_4g,  w, 0.50)                  AS p50_rsrq_4g,
  approx_percentile(rssnr_4g, w, 0.50)                  AS p50_rssnr_4g,
  approx_percentile(rssi,     w, 0.50)                  AS p50_rssi,
  count(rssi)                                           AS n_rssi,
  approx_percentile(ta_4g, w, 0.50)                     AS p50_ta_4g,
  approx_percentile(ta_4g, w, 0.90)                     AS p90_ta_4g,
  approx_percentile(lte_sinr, w, 0.50)                  AS p50_lte_sinr,
  approx_percentile(ss_rsrp_5g,  w, 0.50)               AS p50_ss_rsrp_5g,
  approx_percentile(ss_rsrq_5g,  w, 0.50)               AS p50_ss_rsrq_5g,
  approx_percentile(ss_sinr_5g,  w, 0.50)               AS p50_ss_sinr_5g,
  count(ss_rsrp_5g)                                     AS n_ss_rsrp_5g,
  approx_percentile(csi_rsrp_5g, w, 0.50)               AS p50_csi_rsrp_5g,
  approx_percentile(csi_sinr_5g, w, 0.50)               AS p50_csi_sinr_5g,
  -- ================= MIX RATES (free: columns, not extra rows) ============
  1e2*sum(IF(ip_version='6', w, 0))/nullif(sum(w),0)               AS pct_ipv6,
  1e2*sum(IF(http_stack LIKE '%mvfst%', w, 0))/nullif(sum(w),0)    AS pct_quic,
  1e2*sum(IF(os='iPhone OS', w, 0))/nullif(sum(w),0)               AS pct_ios,
  1e2*sum(IF(os='Android', w, 0))/nullif(sum(w),0)                 AS pct_android,
  1e2*sum(IF(home_country <> country, w, 0))/nullif(sum(w),0)      AS pct_roaming_homecountry,
  1e2*sum(IF(sim_roaming, w, 0))/nullif(sum(w),0)                  AS pct_roaming_simflag,
  1e2*sum(IF(is_nr_nsa_signal_strength, w, 0))/nullif(sum(w),0)    AS pct_nr_signal_reported,
  1e2*sum(IF(dma_consent_status='consented', w, 0))/nullif(sum(w),0) AS pct_dma_consented,
  -- ===== NR SECONDARY CARRIER: the actual 5G link on NSA =====
  1e2*sum(IF(n_nr_sec > 0, w, 0))/nullif(sum(w),0)      AS pct_has_nr_secondary,
  approx_percentile(nr2_ss_rsrp, w, 0.10)               AS nr2_p10_ss_rsrp,
  approx_percentile(nr2_ss_rsrp, w, 0.50)               AS nr2_p50_ss_rsrp,
  approx_percentile(nr2_ss_rsrp, w, 0.90)               AS nr2_p90_ss_rsrp,
  count(nr2_ss_rsrp)                                    AS n_nr2_ss_rsrp,
  approx_percentile(nr2_ss_rsrq, w, 0.50)               AS nr2_p50_ss_rsrq,
  approx_percentile(nr2_ss_sinr, w, 0.50)               AS nr2_p50_ss_sinr,
  approx_percentile(nr2_csi_rsrp, w, 0.50)              AS nr2_p50_csi_rsrp,
  approx_percentile(nr2_csi_sinr, w, 0.50)              AS nr2_p50_csi_sinr,
  approx_distinct(nr2_band)                             AS nr2_distinct_bands,
  approx_distinct(nr2_arfcn)                            AS nr2_distinct_arfcns,
  -- ===== 2G/3G LEVEL SPREAD (no quality metric exists for these RATs) =====
  approx_percentile(rssi, w, 0.10)                      AS p10_rssi,
  approx_percentile(rssi, w, 0.90)                      AS p90_rssi,
  approx_distinct(psc)                                  AS distinct_psc_3g,
  -- ===== CELL RADIO CONFIG =====
  approx_distinct(band)                                 AS distinct_bands,
  approx_distinct(pci)                                  AS distinct_pcis,
  approx_percentile(bandwidth, w, 0.50)                 AS p50_bandwidth,
  -- ===== SIM / SUBSCRIPTION ATTRIBUTES =====
  1e2*sum(IF(dual_sim, w, 0))/nullif(sum(w),0)          AS pct_dual_sim,
  1e2*sum(IF(mvno_or_roam, w, 0))/nullif(sum(w),0)      AS pct_mvno_or_roaming,
  1e2*sum(IF(phone_perm, w, 0))/nullif(sum(w),0)        AS pct_phone_permission
FROM (
  SELECT
    ds,
    CAST(substr(ts, 12, 2) AS integer) AS hour_utc,
    CAST(weight AS bigint) AS w,
    country, home_country, ip_version, http_stack, os, dma_consent_status,
    device_model, connection_subtype, is_nr_nsa_signal_strength,
    request_status, status_code, congestion_state, tower_changed,
    rtt, rtt_variance, ttfb, ttlb, upstream_latency, upload_latency,
    device_lat, device_long, device_acc, device_altitude,
    response_body_compressed_size          AS resp_bytes,
    request_body_size                      AS req_bytes,
    response_header_compressed_size        AS resp_hdr,
    request_header_compressed_size         AS req_hdr,
    element_at(subscription_info.active_subscriptions, 1).is_network_roaming AS sim_roaming,
    IF(total_packets             < 0, NULL, total_packets)             AS tp,
    IF(total_packets_lost        < 0, NULL, total_packets_lost)        AS tpl,
    IF(server_total_packets      < 0, NULL, server_total_packets)      AS stp,
    IF(server_total_packets_lost < 0, NULL, server_total_packets_lost) AS stpl,
    IF(lte_sinr = 2147483647, NULL, lte_sinr) AS lte_sinr,
    cell.mcc AS op_mcc,
    TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.ci AS ci, cell.tac AS tac, cell.arfcn AS arfcn,
    COALESCE(cardinality(nr_secondary_cell_info),0) AS n_nr_sec,
    IF(element_at(nr_secondary_cell_info,1).ss_rsrp  = 2147483647, NULL, element_at(nr_secondary_cell_info,1).ss_rsrp)  AS nr2_ss_rsrp,
    IF(element_at(nr_secondary_cell_info,1).ss_rsrq  = 2147483647, NULL, element_at(nr_secondary_cell_info,1).ss_rsrq)  AS nr2_ss_rsrq,
    IF(element_at(nr_secondary_cell_info,1).ss_sinr  = 2147483647, NULL, element_at(nr_secondary_cell_info,1).ss_sinr)  AS nr2_ss_sinr,
    IF(element_at(nr_secondary_cell_info,1).csi_rsrp = 2147483647, NULL, element_at(nr_secondary_cell_info,1).csi_rsrp) AS nr2_csi_rsrp,
    IF(element_at(nr_secondary_cell_info,1).csi_sinr = 2147483647, NULL, element_at(nr_secondary_cell_info,1).csi_sinr) AS nr2_csi_sinr,
    array_join(element_at(nr_secondary_cell_info,1).bands,'+') AS nr2_band,
    element_at(nr_secondary_cell_info,1).arfcn                 AS nr2_arfcn,
    IF(cell.psc       = 2147483647, NULL, cell.psc)       AS psc,
    IF(cell.bandwidth = 2147483647, NULL, cell.bandwidth) AS bandwidth,
    cell.pci AS pci,
    cardinality(subscription_info.active_subscriptions) > 1 AS dual_sim,
    element_at(subscription_info.active_subscriptions,1).sim_operator_mcc_mnc
      <> element_at(subscription_info.active_subscriptions,1).network_operator_mcc_mnc AS mvno_or_roam,
    subscription_info.has_phone_permission AS phone_perm,
    COALESCE(array_join(cell.bands, '+'), 'unknown') AS band,
    IF(cell.signal_dbm     = 2147483647, NULL, cell.signal_dbm)     AS signal_dbm,
    IF(cell.rsrp           = 2147483647, NULL, cell.rsrp)           AS rsrp_4g,
    IF(cell.rsrq           = 2147483647, NULL, cell.rsrq)           AS rsrq_4g,
    IF(cell.rssnr          = 2147483647, NULL, cell.rssnr)          AS rssnr_4g,
    IF(cell.rssi           = 2147483647, NULL, cell.rssi)           AS rssi,
    IF(cell.timing_advance = 2147483647, NULL, cell.timing_advance) AS ta_4g,
    IF(cell.ss_rsrp        = 2147483647, NULL, cell.ss_rsrp)        AS ss_rsrp_5g,
    IF(cell.ss_rsrq        = 2147483647, NULL, cell.ss_rsrq)        AS ss_rsrq_5g,
    IF(cell.ss_sinr        = 2147483647, NULL, cell.ss_sinr)        AS ss_sinr_5g,
    IF(cell.csi_rsrp       = 2147483647, NULL, cell.csi_rsrp)       AS csi_rsrp_5g,
    IF(cell.csi_sinr       = 2147483647, NULL, cell.csi_sinr)       AS csi_sinr_5g,
    CASE
      WHEN COALESCE(cardinality(nr_secondary_cell_info),0) > 0
        OR override_network_type IN ('NR_NSA','NR_NSA_MMWAVE')
        OR connection_subtype = 'nrnsa'                                      THEN '5G-NSA'
      WHEN cell.network_type='nr'    OR connection_subtype IN ('nr','5gnr','5g','nr-14') THEN '5G-SA'
      WHEN cell.network_type='lte'   OR connection_subtype IN ('lte','lte_ca','4g')      THEN '4G'
      WHEN cell.network_type='wcdma' OR connection_subtype IN ('umts','wcdma','hsdpa',
           'hsupa','hspa','hspa+','h','h+','dc_hspap','cdma - evdo rev. a',
           'cdma - evdo rev','cdma - evdo rev. 0')                                       THEN '3G'
      WHEN cell.network_type='gsm'   OR connection_subtype IN ('gprs','edge','gsm',
           'cdma - 1xrtt','cdma')                                                        THEN '2G'
      ELSE 'other' END AS generation
  FROM (
    SELECT *, COALESCE(
        element_at(filter(registered_cell_info, x -> x.connection_status = 1), 1),
        element_at(registered_cell_info, 1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>'
      AND country = 'GH'
  )

)
GROUP BY 1,2,3,4,5,6

================================================================================
-- QUERY: cube_tac.sql   ->  gh_tac_28d.csv
================================================================================

-- ============================================================================
-- Ghana TAC cube: ds x operator x generation x tac
-- table: cell_tower_info_v2:mobile
-- ----------------------------------------------------------------------------
-- Geography WITHOUT lat/long. `tac` (tracking area code) comes from
-- registered_cell_info, which carries no UDP/DSS4 restriction. Ghana has ~1,787
-- distinct TACs -- FINER than the z=10 quadkey grid (294 tiles) -- so this is
-- the better geographic key unless you specifically need a projected grid.
-- Sentinels guarded (2147483647 signal / -1 packet counters); all measures
-- weighted by `weight`; loss carries raw numerators so it re-aggregates.
-- ============================================================================
SELECT
  ds,
  CASE WHEN op_mcc='620' AND op_mnc=1 THEN 'MTN GH'
       WHEN op_mcc='620' AND op_mnc=2 THEN 'Telecel GH'
       WHEN op_mcc='620' AND op_mnc=3 THEN 'AirtelTigo GH'
       WHEN op_mcc='620'              THEN 'other GH'
       WHEN op_mcc IS NULL            THEN 'unknown/no-cell'
       ELSE 'roaming/foreign' END AS operator,
  op_mcc, op_mnc, generation, tac,
  -- ================= VOLUME / FOOTPRINT =================
  count(*)                                              AS raw_rows,
  sum(w)                                                AS est_sessions,
  approx_distinct(ci)                                   AS cells,
  approx_distinct(tac)                                  AS tacs,
  approx_distinct(arfcn)                                AS arfcns,
  approx_distinct(device_model)                         AS device_models,
  -- ================= LATENCY =================
  approx_percentile(rtt, w, 0.10)                       AS p10_rtt,
  approx_percentile(rtt, w, 0.25)                       AS p25_rtt,
  approx_percentile(rtt, w, 0.50)                       AS p50_rtt,
  approx_percentile(rtt, w, 0.75)                       AS p75_rtt,
  approx_percentile(rtt, w, 0.90)                       AS p90_rtt,
  approx_percentile(rtt, w, 0.95)                       AS p95_rtt,
  approx_percentile(rtt, w, 0.99)                       AS p99_rtt,
  approx_percentile(ttfb, w, 0.50)                      AS p50_ttfb,
  approx_percentile(ttfb, w, 0.90)                      AS p90_ttfb,
  approx_percentile(ttfb, w, 0.95)                      AS p95_ttfb,
  approx_percentile(ttlb, w, 0.50)                      AS p50_ttlb,
  approx_percentile(ttlb, w, 0.90)                      AS p90_ttlb,
  approx_percentile(upstream_latency, w, 0.50)          AS p50_upstream,
  approx_percentile(upstream_latency, w, 0.90)          AS p90_upstream,
  approx_percentile(upload_latency, w, 0.50)            AS p50_upload,
  approx_percentile(upload_latency, w, 0.90)            AS p90_upload,
  approx_percentile(rtt_variance, w, 0.50)              AS p50_rtt_var,
  approx_percentile(rtt_variance, w, 0.90)              AS p90_rtt_var,
  -- ================= LOSS (rates + raw numerators for re-aggregation) ======
  1e2*sum(tpl *w)/nullif(sum(tp *w),0)                  AS loss_pct,
  1e2*sum(stpl*w)/nullif(sum(stp*w),0)                  AS srv_loss_pct,
  sum(tp  *w)                                           AS pkts_w,
  sum(tpl *w)                                           AS pkts_lost_w,
  sum(stp *w)                                           AS srv_pkts_w,
  sum(stpl*w)                                           AS srv_pkts_lost_w,
  1e2*sum(IF(tpl > 0, w, 0))/nullif(sum(IF(tp IS NOT NULL, w, 0)),0) AS pct_sessions_with_loss,
  -- ================= OUTCOMES =================
  1e2*sum(IF(request_status='done', w, 0))/nullif(sum(w),0)        AS ok_pct,
  1e2*sum(IF(status_code >= 500, w, 0))/nullif(sum(w),0)           AS http5xx_pct,
  1e2*sum(IF(status_code BETWEEN 400 AND 499, w, 0))/nullif(sum(w),0) AS http4xx_pct,
  1e2*sum(IF(tower_changed, w, 0))/nullif(sum(w),0)                AS tower_changed_pct,
  approx_percentile(congestion_state, w, 0.50)          AS p50_congestion,
  1e2*sum(IF(congestion_state > 1, w, 0))/nullif(sum(w),0)         AS pct_congested,
  -- ================= BYTES / THROUGHPUT =================
  approx_percentile(resp_bytes, w, 0.50)                AS p50_resp_bytes,
  approx_percentile(resp_bytes, w, 0.90)                AS p90_resp_bytes,
  approx_percentile(req_bytes,  w, 0.50)                AS p50_req_bytes,
  approx_percentile(resp_hdr,   w, 0.50)                AS p50_resp_hdr_bytes,
  approx_percentile(req_hdr,    w, 0.50)                AS p50_req_hdr_bytes,
  sum(resp_bytes * w)                                   AS resp_bytes_w,
  sum(req_bytes  * w)                                   AS req_bytes_w,
  -- ================= SIGNAL (RAT-specific; n_* are denominators) ==========
  approx_percentile(signal_dbm, w, 0.10)                AS p10_signal_dbm,
  approx_percentile(signal_dbm, w, 0.50)                AS p50_signal_dbm,
  approx_percentile(signal_dbm, w, 0.90)                AS p90_signal_dbm,
  count(signal_dbm)                                     AS n_signal_dbm,
  approx_percentile(rsrp_4g, w, 0.10)                   AS p10_rsrp_4g,
  approx_percentile(rsrp_4g, w, 0.50)                   AS p50_rsrp_4g,
  approx_percentile(rsrp_4g, w, 0.90)                   AS p90_rsrp_4g,
  count(rsrp_4g)                                        AS n_rsrp_4g,
  approx_percentile(rsrq_4g,  w, 0.50)                  AS p50_rsrq_4g,
  approx_percentile(rssnr_4g, w, 0.50)                  AS p50_rssnr_4g,
  approx_percentile(rssi,     w, 0.50)                  AS p50_rssi,
  count(rssi)                                           AS n_rssi,
  approx_percentile(ta_4g, w, 0.50)                     AS p50_ta_4g,
  approx_percentile(ta_4g, w, 0.90)                     AS p90_ta_4g,
  approx_percentile(lte_sinr, w, 0.50)                  AS p50_lte_sinr,
  approx_percentile(ss_rsrp_5g,  w, 0.50)               AS p50_ss_rsrp_5g,
  approx_percentile(ss_rsrq_5g,  w, 0.50)               AS p50_ss_rsrq_5g,
  approx_percentile(ss_sinr_5g,  w, 0.50)               AS p50_ss_sinr_5g,
  count(ss_rsrp_5g)                                     AS n_ss_rsrp_5g,
  approx_percentile(csi_rsrp_5g, w, 0.50)               AS p50_csi_rsrp_5g,
  approx_percentile(csi_sinr_5g, w, 0.50)               AS p50_csi_sinr_5g,
  -- ================= MIX RATES (free: columns, not extra rows) ============
  1e2*sum(IF(ip_version='6', w, 0))/nullif(sum(w),0)               AS pct_ipv6,
  1e2*sum(IF(http_stack LIKE '%mvfst%', w, 0))/nullif(sum(w),0)    AS pct_quic,
  1e2*sum(IF(os='iPhone OS', w, 0))/nullif(sum(w),0)               AS pct_ios,
  1e2*sum(IF(os='Android', w, 0))/nullif(sum(w),0)                 AS pct_android,
  1e2*sum(IF(home_country <> country, w, 0))/nullif(sum(w),0)      AS pct_roaming_homecountry,
  1e2*sum(IF(sim_roaming, w, 0))/nullif(sum(w),0)                  AS pct_roaming_simflag,
  1e2*sum(IF(is_nr_nsa_signal_strength, w, 0))/nullif(sum(w),0)    AS pct_nr_signal_reported,
  1e2*sum(IF(dma_consent_status='consented', w, 0))/nullif(sum(w),0) AS pct_dma_consented
FROM (
  SELECT
    ds,
    CAST(substr(ts, 12, 2) AS integer) AS hour_utc,
    CAST(weight AS bigint) AS w,
    country, home_country, ip_version, http_stack, os, dma_consent_status,
    device_model, connection_subtype, is_nr_nsa_signal_strength,
    request_status, status_code, congestion_state, tower_changed,
    rtt, rtt_variance, ttfb, ttlb, upstream_latency, upload_latency,
    device_lat, device_long, device_acc, device_altitude,
    response_body_compressed_size          AS resp_bytes,
    request_body_size                      AS req_bytes,
    response_header_compressed_size        AS resp_hdr,
    request_header_compressed_size         AS req_hdr,
    element_at(subscription_info.active_subscriptions, 1).is_network_roaming AS sim_roaming,
    IF(total_packets             < 0, NULL, total_packets)             AS tp,
    IF(total_packets_lost        < 0, NULL, total_packets_lost)        AS tpl,
    IF(server_total_packets      < 0, NULL, server_total_packets)      AS stp,
    IF(server_total_packets_lost < 0, NULL, server_total_packets_lost) AS stpl,
    IF(lte_sinr = 2147483647, NULL, lte_sinr) AS lte_sinr,
    cell.mcc AS op_mcc,
    TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.ci AS ci, cell.tac AS tac, cell.arfcn AS arfcn,
    COALESCE(array_join(cell.bands, '+'), 'unknown') AS band,
    IF(cell.signal_dbm     = 2147483647, NULL, cell.signal_dbm)     AS signal_dbm,
    IF(cell.rsrp           = 2147483647, NULL, cell.rsrp)           AS rsrp_4g,
    IF(cell.rsrq           = 2147483647, NULL, cell.rsrq)           AS rsrq_4g,
    IF(cell.rssnr          = 2147483647, NULL, cell.rssnr)          AS rssnr_4g,
    IF(cell.rssi           = 2147483647, NULL, cell.rssi)           AS rssi,
    IF(cell.timing_advance = 2147483647, NULL, cell.timing_advance) AS ta_4g,
    IF(cell.ss_rsrp        = 2147483647, NULL, cell.ss_rsrp)        AS ss_rsrp_5g,
    IF(cell.ss_rsrq        = 2147483647, NULL, cell.ss_rsrq)        AS ss_rsrq_5g,
    IF(cell.ss_sinr        = 2147483647, NULL, cell.ss_sinr)        AS ss_sinr_5g,
    IF(cell.csi_rsrp       = 2147483647, NULL, cell.csi_rsrp)       AS csi_rsrp_5g,
    IF(cell.csi_sinr       = 2147483647, NULL, cell.csi_sinr)       AS csi_sinr_5g,
    CASE
      WHEN COALESCE(cardinality(nr_secondary_cell_info),0) > 0
        OR override_network_type IN ('NR_NSA','NR_NSA_MMWAVE')
        OR connection_subtype = 'nrnsa'                                      THEN '5G-NSA'
      WHEN cell.network_type='nr'    OR connection_subtype IN ('nr','5gnr','5g','nr-14') THEN '5G-SA'
      WHEN cell.network_type='lte'   OR connection_subtype IN ('lte','lte_ca','4g')      THEN '4G'
      WHEN cell.network_type='wcdma' OR connection_subtype IN ('umts','wcdma','hsdpa',
           'hsupa','hspa','hspa+','h','h+','dc_hspap','cdma - evdo rev. a',
           'cdma - evdo rev','cdma - evdo rev. 0')                                       THEN '3G'
      WHEN cell.network_type='gsm'   OR connection_subtype IN ('gprs','edge','gsm',
           'cdma - 1xrtt','cdma')                                                        THEN '2G'
      ELSE 'other' END AS generation
  FROM (
    SELECT *, COALESCE(
        element_at(filter(registered_cell_info, x -> x.connection_status = 1), 1),
        element_at(registered_cell_info, 1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>'
      AND country = 'GH'
  )

)
GROUP BY 1,2,3,4,5,6

================================================================================
-- QUERY: cube_diurnal.sql   ->  gh_diurnal_28d.csv
================================================================================

-- ============================================================================
-- Ghana DIURNAL cube: ds x hour x operator x generation x connection_subtype
-- table: cell_tower_info_v2:mobile   |   no geography, no lat/long dependency
-- Sentinels guarded (2147483647 signal / -1 packet counters); all measures
-- weighted by `weight`. Loss carries raw numerators so it re-aggregates.
-- ============================================================================
SELECT
  ds, hour_utc,
  CASE WHEN op_mcc='620' AND op_mnc=1 THEN 'MTN GH'
       WHEN op_mcc='620' AND op_mnc=2 THEN 'Telecel GH'
       WHEN op_mcc='620' AND op_mnc=3 THEN 'AirtelTigo GH'
       WHEN op_mcc='620'              THEN 'other GH'
       WHEN op_mcc IS NULL            THEN 'unknown/no-cell'
       ELSE 'roaming/foreign' END AS operator,
  op_mcc, op_mnc, generation, connection_subtype,
  -- ================= VOLUME / FOOTPRINT =================
  count(*)                                              AS raw_rows,
  sum(w)                                                AS est_sessions,
  approx_distinct(ci)                                   AS cells,
  approx_distinct(tac)                                  AS tacs,
  approx_distinct(arfcn)                                AS arfcns,
  approx_distinct(device_model)                         AS device_models,
  -- ================= LATENCY =================
  approx_percentile(rtt, w, 0.10)                       AS p10_rtt,
  approx_percentile(rtt, w, 0.25)                       AS p25_rtt,
  approx_percentile(rtt, w, 0.50)                       AS p50_rtt,
  approx_percentile(rtt, w, 0.75)                       AS p75_rtt,
  approx_percentile(rtt, w, 0.90)                       AS p90_rtt,
  approx_percentile(rtt, w, 0.95)                       AS p95_rtt,
  approx_percentile(rtt, w, 0.99)                       AS p99_rtt,
  approx_percentile(ttfb, w, 0.50)                      AS p50_ttfb,
  approx_percentile(ttfb, w, 0.90)                      AS p90_ttfb,
  approx_percentile(ttfb, w, 0.95)                      AS p95_ttfb,
  approx_percentile(ttlb, w, 0.50)                      AS p50_ttlb,
  approx_percentile(ttlb, w, 0.90)                      AS p90_ttlb,
  approx_percentile(upstream_latency, w, 0.50)          AS p50_upstream,
  approx_percentile(upstream_latency, w, 0.90)          AS p90_upstream,
  approx_percentile(upload_latency, w, 0.50)            AS p50_upload,
  approx_percentile(upload_latency, w, 0.90)            AS p90_upload,
  approx_percentile(rtt_variance, w, 0.50)              AS p50_rtt_var,
  approx_percentile(rtt_variance, w, 0.90)              AS p90_rtt_var,
  -- ================= LOSS (rates + raw numerators for re-aggregation) ======
  1e2*sum(tpl *w)/nullif(sum(tp *w),0)                  AS loss_pct,
  1e2*sum(stpl*w)/nullif(sum(stp*w),0)                  AS srv_loss_pct,
  sum(tp  *w)                                           AS pkts_w,
  sum(tpl *w)                                           AS pkts_lost_w,
  sum(stp *w)                                           AS srv_pkts_w,
  sum(stpl*w)                                           AS srv_pkts_lost_w,
  1e2*sum(IF(tpl > 0, w, 0))/nullif(sum(IF(tp IS NOT NULL, w, 0)),0) AS pct_sessions_with_loss,
  -- ================= OUTCOMES =================
  1e2*sum(IF(request_status='done', w, 0))/nullif(sum(w),0)        AS ok_pct,
  1e2*sum(IF(status_code >= 500, w, 0))/nullif(sum(w),0)           AS http5xx_pct,
  1e2*sum(IF(status_code BETWEEN 400 AND 499, w, 0))/nullif(sum(w),0) AS http4xx_pct,
  1e2*sum(IF(tower_changed, w, 0))/nullif(sum(w),0)                AS tower_changed_pct,
  approx_percentile(congestion_state, w, 0.50)          AS p50_congestion,
  1e2*sum(IF(congestion_state > 1, w, 0))/nullif(sum(w),0)         AS pct_congested,
  -- ================= BYTES / THROUGHPUT =================
  approx_percentile(resp_bytes, w, 0.50)                AS p50_resp_bytes,
  approx_percentile(resp_bytes, w, 0.90)                AS p90_resp_bytes,
  approx_percentile(req_bytes,  w, 0.50)                AS p50_req_bytes,
  approx_percentile(resp_hdr,   w, 0.50)                AS p50_resp_hdr_bytes,
  approx_percentile(req_hdr,    w, 0.50)                AS p50_req_hdr_bytes,
  sum(resp_bytes * w)                                   AS resp_bytes_w,
  sum(req_bytes  * w)                                   AS req_bytes_w,
  -- ================= SIGNAL (RAT-specific; n_* are denominators) ==========
  approx_percentile(signal_dbm, w, 0.10)                AS p10_signal_dbm,
  approx_percentile(signal_dbm, w, 0.50)                AS p50_signal_dbm,
  approx_percentile(signal_dbm, w, 0.90)                AS p90_signal_dbm,
  count(signal_dbm)                                     AS n_signal_dbm,
  approx_percentile(rsrp_4g, w, 0.10)                   AS p10_rsrp_4g,
  approx_percentile(rsrp_4g, w, 0.50)                   AS p50_rsrp_4g,
  approx_percentile(rsrp_4g, w, 0.90)                   AS p90_rsrp_4g,
  count(rsrp_4g)                                        AS n_rsrp_4g,
  approx_percentile(rsrq_4g,  w, 0.50)                  AS p50_rsrq_4g,
  approx_percentile(rssnr_4g, w, 0.50)                  AS p50_rssnr_4g,
  approx_percentile(rssi,     w, 0.50)                  AS p50_rssi,
  count(rssi)                                           AS n_rssi,
  approx_percentile(ta_4g, w, 0.50)                     AS p50_ta_4g,
  approx_percentile(ta_4g, w, 0.90)                     AS p90_ta_4g,
  approx_percentile(lte_sinr, w, 0.50)                  AS p50_lte_sinr,
  approx_percentile(ss_rsrp_5g,  w, 0.50)               AS p50_ss_rsrp_5g,
  approx_percentile(ss_rsrq_5g,  w, 0.50)               AS p50_ss_rsrq_5g,
  approx_percentile(ss_sinr_5g,  w, 0.50)               AS p50_ss_sinr_5g,
  count(ss_rsrp_5g)                                     AS n_ss_rsrp_5g,
  approx_percentile(csi_rsrp_5g, w, 0.50)               AS p50_csi_rsrp_5g,
  approx_percentile(csi_sinr_5g, w, 0.50)               AS p50_csi_sinr_5g,
  -- ================= MIX RATES (free: columns, not extra rows) ============
  1e2*sum(IF(ip_version='6', w, 0))/nullif(sum(w),0)               AS pct_ipv6,
  1e2*sum(IF(http_stack LIKE '%mvfst%', w, 0))/nullif(sum(w),0)    AS pct_quic,
  1e2*sum(IF(os='iPhone OS', w, 0))/nullif(sum(w),0)               AS pct_ios,
  1e2*sum(IF(os='Android', w, 0))/nullif(sum(w),0)                 AS pct_android,
  1e2*sum(IF(home_country <> country, w, 0))/nullif(sum(w),0)      AS pct_roaming_homecountry,
  1e2*sum(IF(sim_roaming, w, 0))/nullif(sum(w),0)                  AS pct_roaming_simflag,
  1e2*sum(IF(is_nr_nsa_signal_strength, w, 0))/nullif(sum(w),0)    AS pct_nr_signal_reported,
  1e2*sum(IF(dma_consent_status='consented', w, 0))/nullif(sum(w),0) AS pct_dma_consented
FROM (
  SELECT
    ds,
    CAST(substr(ts, 12, 2) AS integer) AS hour_utc,
    CAST(weight AS bigint) AS w,
    country, home_country, ip_version, http_stack, os, dma_consent_status,
    device_model, connection_subtype, is_nr_nsa_signal_strength,
    request_status, status_code, congestion_state, tower_changed,
    rtt, rtt_variance, ttfb, ttlb, upstream_latency, upload_latency,
    device_lat, device_long, device_acc, device_altitude,
    response_body_compressed_size          AS resp_bytes,
    request_body_size                      AS req_bytes,
    response_header_compressed_size        AS resp_hdr,
    request_header_compressed_size         AS req_hdr,
    element_at(subscription_info.active_subscriptions, 1).is_network_roaming AS sim_roaming,
    IF(total_packets             < 0, NULL, total_packets)             AS tp,
    IF(total_packets_lost        < 0, NULL, total_packets_lost)        AS tpl,
    IF(server_total_packets      < 0, NULL, server_total_packets)      AS stp,
    IF(server_total_packets_lost < 0, NULL, server_total_packets_lost) AS stpl,
    IF(lte_sinr = 2147483647, NULL, lte_sinr) AS lte_sinr,
    cell.mcc AS op_mcc,
    TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.ci AS ci, cell.tac AS tac, cell.arfcn AS arfcn,
    COALESCE(array_join(cell.bands, '+'), 'unknown') AS band,
    IF(cell.signal_dbm     = 2147483647, NULL, cell.signal_dbm)     AS signal_dbm,
    IF(cell.rsrp           = 2147483647, NULL, cell.rsrp)           AS rsrp_4g,
    IF(cell.rsrq           = 2147483647, NULL, cell.rsrq)           AS rsrq_4g,
    IF(cell.rssnr          = 2147483647, NULL, cell.rssnr)          AS rssnr_4g,
    IF(cell.rssi           = 2147483647, NULL, cell.rssi)           AS rssi,
    IF(cell.timing_advance = 2147483647, NULL, cell.timing_advance) AS ta_4g,
    IF(cell.ss_rsrp        = 2147483647, NULL, cell.ss_rsrp)        AS ss_rsrp_5g,
    IF(cell.ss_rsrq        = 2147483647, NULL, cell.ss_rsrq)        AS ss_rsrq_5g,
    IF(cell.ss_sinr        = 2147483647, NULL, cell.ss_sinr)        AS ss_sinr_5g,
    IF(cell.csi_rsrp       = 2147483647, NULL, cell.csi_rsrp)       AS csi_rsrp_5g,
    IF(cell.csi_sinr       = 2147483647, NULL, cell.csi_sinr)       AS csi_sinr_5g,
    CASE
      WHEN COALESCE(cardinality(nr_secondary_cell_info),0) > 0
        OR override_network_type IN ('NR_NSA','NR_NSA_MMWAVE')
        OR connection_subtype = 'nrnsa'                                      THEN '5G-NSA'
      WHEN cell.network_type='nr'    OR connection_subtype IN ('nr','5gnr','5g','nr-14') THEN '5G-SA'
      WHEN cell.network_type='lte'   OR connection_subtype IN ('lte','lte_ca','4g')      THEN '4G'
      WHEN cell.network_type='wcdma' OR connection_subtype IN ('umts','wcdma','hsdpa',
           'hsupa','hspa','hspa+','h','h+','dc_hspap','cdma - evdo rev. a',
           'cdma - evdo rev','cdma - evdo rev. 0')                                       THEN '3G'
      WHEN cell.network_type='gsm'   OR connection_subtype IN ('gprs','edge','gsm',
           'cdma - 1xrtt','cdma')                                                        THEN '2G'
      ELSE 'other' END AS generation
  FROM (
    SELECT *, COALESCE(
        element_at(filter(registered_cell_info, x -> x.connection_status = 1), 1),
        element_at(registered_cell_info, 1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>'
      AND country = 'GH'
  )

)
GROUP BY 1,2,3,4,5,6,7

================================================================================
-- QUERY: tac_centroid.sql   ->  gh_tac_centroids.csv
================================================================================

-- Ghana TAC -> location lookup. Join this to gh_tac_28d.csv on (op_mcc, op_mnc, tac)
-- to make the TAC cube mappable. TAC is operator-scoped, so all three keys matter.
--
-- Small by design: sized under Daiquery's 1,000-row display limit, so
-- Export -> CSV (DISPLAYED Dataset) downloads instantly. Nothing is written,
-- so no Chronos job, no oncall requirement and no GDA propagation.
SELECT
  op_mcc, op_mnc, tac,
  CASE WHEN op_mcc='620' AND op_mnc=1 THEN 'MTN GH'
       WHEN op_mcc='620' AND op_mnc=2 THEN 'Telecel GH'
       WHEN op_mcc='620' AND op_mnc=3 THEN 'AirtelTigo GH'
       WHEN op_mcc='620'              THEN 'other GH'
       ELSE 'roaming/foreign' END                  AS operator,
  count(*)                                         AS raw_rows,
  sum(w)                                           AS est_sessions,
  approx_distinct(ci)                              AS cells,
  -- centroid to plot
  approx_percentile(device_lat,  w, 0.50)          AS centroid_lat,
  approx_percentile(device_long, w, 0.50)          AS centroid_long,
  -- extent, so you can size the marker / spot TACs that are smeared
  approx_percentile(device_lat,  w, 0.10)          AS lat_p10,
  approx_percentile(device_lat,  w, 0.90)          AS lat_p90,
  approx_percentile(device_long, w, 0.10)          AS long_p10,
  approx_percentile(device_long, w, 0.90)          AS long_p90,
  approx_percentile(device_acc,  w, 0.50)          AS p50_gps_acc_m,
  1e2*count(device_lat)/count(*)                   AS latlong_coverage_pct
FROM (
  SELECT CAST(weight AS bigint) AS w, device_lat, device_long, device_acc,
    cell.mcc AS op_mcc, TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.tac AS tac, cell.ci AS ci
  FROM (
    SELECT *, COALESCE(element_at(filter(registered_cell_info, x -> x.connection_status=1),1),
                       element_at(registered_cell_info,1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>' AND country='GH'
  )
)
WHERE tac IS NOT NULL
GROUP BY 1,2,3,4
HAVING sum(w) > 1000000
ORDER BY est_sessions DESC
LIMIT 900


================================================================================
-- QUERY: geo_small.sql   ->  (not extracted - DSS4)
================================================================================

-- Ghana quadkey map view: POOLED over 28 days, z=8 (155.8 km tiles).
-- FULL measure set. Keys are operator x generation x quadkey only, so the
-- row count stays a few hundred -- under Daiquery's 1,000-row display limit
-- and well under the 240K-cell cap. That means Export -> CSV (DISPLAYED
-- Dataset) works instantly: no Chronos, no oncall, no GDA (nothing written).
--
-- CAUTION on zoom: z=10 (294 tiles) can push rows past 1,000 and cells past
-- 240K with this many columns. If you raise the zoom, trim measures or
-- tighten the HAVING floor.
-- ============================================================================
-- Ghana GEO cube: ds x operator x generation x band x quadkey
-- table: cell_tower_info_v2:mobile
-- ----------------------------------------------------------------------------
-- ZOOM in the `zoom` CTE:  z=8 ->155.8km/24 tiles | z=10 ->38.9km/294 (default)
--                          z=12 ->9.7km/4056.  Do NOT go z>=14: that is the
--   granularity Meta protects via quadkey_14/16/18, which are UDP-denied.
-- Tiles are derived from device_lat/device_long, NOT from the quadkey_* columns
--   (truncating those does not work -- the ACL fires on column read).
-- Sentinels guarded (2147483647 signal / -1 packet counters). All measures are
--   weighted by `weight`; loss carries raw numerators so it re-aggregates.
-- ============================================================================
WITH zoom AS (SELECT 8 AS z),
base AS (
  SELECT
    ds,
    CAST(substr(ts, 12, 2) AS integer) AS hour_utc,
    CAST(weight AS bigint) AS w,
    country, home_country, ip_version, http_stack, os, dma_consent_status,
    device_model, connection_subtype, is_nr_nsa_signal_strength,
    request_status, status_code, congestion_state, tower_changed,
    rtt, rtt_variance, ttfb, ttlb, upstream_latency, upload_latency,
    device_lat, device_long, device_acc, device_altitude,
    response_body_compressed_size          AS resp_bytes,
    request_body_size                      AS req_bytes,
    response_header_compressed_size        AS resp_hdr,
    request_header_compressed_size         AS req_hdr,
    element_at(subscription_info.active_subscriptions, 1).is_network_roaming AS sim_roaming,
    IF(total_packets             < 0, NULL, total_packets)             AS tp,
    IF(total_packets_lost        < 0, NULL, total_packets_lost)        AS tpl,
    IF(server_total_packets      < 0, NULL, server_total_packets)      AS stp,
    IF(server_total_packets_lost < 0, NULL, server_total_packets_lost) AS stpl,
    IF(lte_sinr = 2147483647, NULL, lte_sinr) AS lte_sinr,
    cell.mcc AS op_mcc,
    TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.ci AS ci, cell.tac AS tac, cell.arfcn AS arfcn,
    COALESCE(array_join(cell.bands, '+'), 'unknown') AS band,
    IF(cell.signal_dbm     = 2147483647, NULL, cell.signal_dbm)     AS signal_dbm,
    IF(cell.rsrp           = 2147483647, NULL, cell.rsrp)           AS rsrp_4g,
    IF(cell.rsrq           = 2147483647, NULL, cell.rsrq)           AS rsrq_4g,
    IF(cell.rssnr          = 2147483647, NULL, cell.rssnr)          AS rssnr_4g,
    IF(cell.rssi           = 2147483647, NULL, cell.rssi)           AS rssi,
    IF(cell.timing_advance = 2147483647, NULL, cell.timing_advance) AS ta_4g,
    IF(cell.ss_rsrp        = 2147483647, NULL, cell.ss_rsrp)        AS ss_rsrp_5g,
    IF(cell.ss_rsrq        = 2147483647, NULL, cell.ss_rsrq)        AS ss_rsrq_5g,
    IF(cell.ss_sinr        = 2147483647, NULL, cell.ss_sinr)        AS ss_sinr_5g,
    IF(cell.csi_rsrp       = 2147483647, NULL, cell.csi_rsrp)       AS csi_rsrp_5g,
    IF(cell.csi_sinr       = 2147483647, NULL, cell.csi_sinr)       AS csi_sinr_5g,
    CASE
      WHEN COALESCE(cardinality(nr_secondary_cell_info),0) > 0
        OR override_network_type IN ('NR_NSA','NR_NSA_MMWAVE')
        OR connection_subtype = 'nrnsa'                                      THEN '5G-NSA'
      WHEN cell.network_type='nr'    OR connection_subtype IN ('nr','5gnr','5g','nr-14') THEN '5G-SA'
      WHEN cell.network_type='lte'   OR connection_subtype IN ('lte','lte_ca','4g')      THEN '4G'
      WHEN cell.network_type='wcdma' OR connection_subtype IN ('umts','wcdma','hsdpa',
           'hsupa','hspa','hspa+','h','h+','dc_hspap','cdma - evdo rev. a',
           'cdma - evdo rev','cdma - evdo rev. 0')                                       THEN '3G'
      WHEN cell.network_type='gsm'   OR connection_subtype IN ('gprs','edge','gsm',
           'cdma - 1xrtt','cdma')                                                        THEN '2G'
      ELSE 'other' END AS generation
  FROM (
    SELECT *, COALESCE(
        element_at(filter(registered_cell_info, x -> x.connection_status = 1), 1),
        element_at(registered_cell_info, 1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>'
      -- Corrupt ORC stripe in this one partition (ZSTD "Data corruption
      -- detected", stripe 1 / stream id=705 / node=22). Reported against
      -- cell_tower_info_v2 owner+oncall partner_analytics_instrumentation.
      -- Costs 1 hour of 672 = 0.15% of the window. Remove this line once the
      -- file is repaired, or if a retry succeeds.
      AND ts <> '2026-09-27+20:00:99'
      AND country = 'GH'
  )

),
clipped AS (
  -- Web Mercator is undefined at the poles: at lat = +/-90, sin(lat) = +/-1, so
  -- (1 - sin) = 0, ln() goes infinite and the cast to BIGINT fails. Clip to
  -- +/-85.05112878 (the standard Web Mercator limit) and reject non-finite /
  -- out-of-range coordinates. NULL propagates to a NULL quadkey bucket.
  SELECT b.*, z,
    CASE WHEN device_lat IS NULL OR NOT is_finite(device_lat)
              OR device_lat < -90 OR device_lat > 90 THEN NULL
         ELSE least(greatest(device_lat, -85.05112878), 85.05112878) END AS lat_c,
    CASE WHEN device_long IS NULL OR NOT is_finite(device_long)
              OR device_long < -180 OR device_long > 180 THEN NULL
         ELSE device_long END AS lon_c
  FROM base b CROSS JOIN zoom
),
tiled AS (
  -- Clamp tile indices to [0, 2^z - 1]: lon = +180 lands exactly on 2^z.
  SELECT c.*,
    least(greatest(CAST(floor(((lon_c + 180.0)/360.0) * pow(2,z)) AS bigint), 0),
          CAST(pow(2,z) AS bigint) - 1) AS tile_x,
    least(greatest(CAST(floor((0.5 - ln((1 + sin(lat_c*pi()/180))
                                      / (1 - sin(lat_c*pi()/180))) / (4*pi())) * pow(2,z)) AS bigint), 0),
          CAST(pow(2,z) AS bigint) - 1) AS tile_y
  FROM clipped c
)
SELECT
  CASE WHEN op_mcc='620' AND op_mnc=1 THEN 'MTN GH'
       WHEN op_mcc='620' AND op_mnc=2 THEN 'Telecel GH'
       WHEN op_mcc='620' AND op_mnc=3 THEN 'AirtelTigo GH'
       WHEN op_mcc='620'              THEN 'other GH'
       WHEN op_mcc IS NULL            THEN 'unknown/no-cell'
       ELSE 'roaming/foreign' END AS operator,
  generation,
  z AS zoom,
  -- NULL tile must stay NULL: bitwise_and(NULL,..) would otherwise render as '000..'
  IF(tile_x IS NULL OR tile_y IS NULL, NULL,
    array_join(transform(sequence(z,1,-1), i ->
      CAST( IF(bitwise_and(tile_x, CAST(pow(2,i-1) AS bigint)) <> 0, 1, 0)
          + IF(bitwise_and(tile_y, CAST(pow(2,i-1) AS bigint)) <> 0, 2, 0) AS varchar)), '')) AS quadkey,
  -- ================= VOLUME / FOOTPRINT =================
  count(*)                                              AS raw_rows,
  sum(w)                                                AS est_sessions,
  approx_distinct(ci)                                   AS cells,
  approx_distinct(tac)                                  AS tacs,
  approx_distinct(arfcn)                                AS arfcns,
  approx_distinct(device_model)                         AS device_models,
  -- ================= LATENCY =================
  approx_percentile(rtt, w, 0.10)                       AS p10_rtt,
  approx_percentile(rtt, w, 0.25)                       AS p25_rtt,
  approx_percentile(rtt, w, 0.50)                       AS p50_rtt,
  approx_percentile(rtt, w, 0.75)                       AS p75_rtt,
  approx_percentile(rtt, w, 0.90)                       AS p90_rtt,
  approx_percentile(rtt, w, 0.95)                       AS p95_rtt,
  approx_percentile(rtt, w, 0.99)                       AS p99_rtt,
  approx_percentile(ttfb, w, 0.50)                      AS p50_ttfb,
  approx_percentile(ttfb, w, 0.90)                      AS p90_ttfb,
  approx_percentile(ttfb, w, 0.95)                      AS p95_ttfb,
  approx_percentile(ttlb, w, 0.50)                      AS p50_ttlb,
  approx_percentile(ttlb, w, 0.90)                      AS p90_ttlb,
  approx_percentile(upstream_latency, w, 0.50)          AS p50_upstream,
  approx_percentile(upstream_latency, w, 0.90)          AS p90_upstream,
  approx_percentile(upload_latency, w, 0.50)            AS p50_upload,
  approx_percentile(upload_latency, w, 0.90)            AS p90_upload,
  approx_percentile(rtt_variance, w, 0.50)              AS p50_rtt_var,
  approx_percentile(rtt_variance, w, 0.90)              AS p90_rtt_var,
  -- ================= LOSS (rates + raw numerators for re-aggregation) ======
  1e2*sum(tpl *w)/nullif(sum(tp *w),0)                  AS loss_pct,
  1e2*sum(stpl*w)/nullif(sum(stp*w),0)                  AS srv_loss_pct,
  sum(tp  *w)                                           AS pkts_w,
  sum(tpl *w)                                           AS pkts_lost_w,
  sum(stp *w)                                           AS srv_pkts_w,
  sum(stpl*w)                                           AS srv_pkts_lost_w,
  1e2*sum(IF(tpl > 0, w, 0))/nullif(sum(IF(tp IS NOT NULL, w, 0)),0) AS pct_sessions_with_loss,
  -- ================= OUTCOMES =================
  1e2*sum(IF(request_status='done', w, 0))/nullif(sum(w),0)        AS ok_pct,
  1e2*sum(IF(status_code >= 500, w, 0))/nullif(sum(w),0)           AS http5xx_pct,
  1e2*sum(IF(status_code BETWEEN 400 AND 499, w, 0))/nullif(sum(w),0) AS http4xx_pct,
  1e2*sum(IF(tower_changed, w, 0))/nullif(sum(w),0)                AS tower_changed_pct,
  approx_percentile(congestion_state, w, 0.50)          AS p50_congestion,
  1e2*sum(IF(congestion_state > 1, w, 0))/nullif(sum(w),0)         AS pct_congested,
  -- ================= BYTES / THROUGHPUT =================
  approx_percentile(resp_bytes, w, 0.50)                AS p50_resp_bytes,
  approx_percentile(resp_bytes, w, 0.90)                AS p90_resp_bytes,
  approx_percentile(req_bytes,  w, 0.50)                AS p50_req_bytes,
  approx_percentile(resp_hdr,   w, 0.50)                AS p50_resp_hdr_bytes,
  approx_percentile(req_hdr,    w, 0.50)                AS p50_req_hdr_bytes,
  sum(resp_bytes * w)                                   AS resp_bytes_w,
  sum(req_bytes  * w)                                   AS req_bytes_w,
  -- ================= SIGNAL (RAT-specific; n_* are denominators) ==========
  approx_percentile(signal_dbm, w, 0.10)                AS p10_signal_dbm,
  approx_percentile(signal_dbm, w, 0.50)                AS p50_signal_dbm,
  approx_percentile(signal_dbm, w, 0.90)                AS p90_signal_dbm,
  count(signal_dbm)                                     AS n_signal_dbm,
  approx_percentile(rsrp_4g, w, 0.10)                   AS p10_rsrp_4g,
  approx_percentile(rsrp_4g, w, 0.50)                   AS p50_rsrp_4g,
  approx_percentile(rsrp_4g, w, 0.90)                   AS p90_rsrp_4g,
  count(rsrp_4g)                                        AS n_rsrp_4g,
  approx_percentile(rsrq_4g,  w, 0.50)                  AS p50_rsrq_4g,
  approx_percentile(rssnr_4g, w, 0.50)                  AS p50_rssnr_4g,
  approx_percentile(rssi,     w, 0.50)                  AS p50_rssi,
  count(rssi)                                           AS n_rssi,
  approx_percentile(ta_4g, w, 0.50)                     AS p50_ta_4g,
  approx_percentile(ta_4g, w, 0.90)                     AS p90_ta_4g,
  approx_percentile(lte_sinr, w, 0.50)                  AS p50_lte_sinr,
  approx_percentile(ss_rsrp_5g,  w, 0.50)               AS p50_ss_rsrp_5g,
  approx_percentile(ss_rsrq_5g,  w, 0.50)               AS p50_ss_rsrq_5g,
  approx_percentile(ss_sinr_5g,  w, 0.50)               AS p50_ss_sinr_5g,
  count(ss_rsrp_5g)                                     AS n_ss_rsrp_5g,
  approx_percentile(csi_rsrp_5g, w, 0.50)               AS p50_csi_rsrp_5g,
  approx_percentile(csi_sinr_5g, w, 0.50)               AS p50_csi_sinr_5g,
  -- ================= MIX RATES (free: columns, not extra rows) ============
  1e2*sum(IF(ip_version='6', w, 0))/nullif(sum(w),0)               AS pct_ipv6,
  1e2*sum(IF(http_stack LIKE '%mvfst%', w, 0))/nullif(sum(w),0)    AS pct_quic,
  1e2*sum(IF(os='iPhone OS', w, 0))/nullif(sum(w),0)               AS pct_ios,
  1e2*sum(IF(os='Android', w, 0))/nullif(sum(w),0)                 AS pct_android,
  1e2*sum(IF(home_country <> country, w, 0))/nullif(sum(w),0)      AS pct_roaming_homecountry,
  1e2*sum(IF(sim_roaming, w, 0))/nullif(sum(w),0)                  AS pct_roaming_simflag,
  1e2*sum(IF(is_nr_nsa_signal_strength, w, 0))/nullif(sum(w),0)    AS pct_nr_signal_reported,
  1e2*sum(IF(dma_consent_status='consented', w, 0))/nullif(sum(w),0) AS pct_dma_consented,
  -- ================= LOCATION =================
  approx_percentile(device_lat,  w, 0.50)               AS median_lat,
  approx_percentile(device_long, w, 0.50)               AS median_long,
  approx_percentile(device_acc,  w, 0.50)               AS p50_gps_acc_m,
  approx_percentile(device_acc,  w, 0.90)               AS p90_gps_acc_m,
  approx_percentile(device_altitude, w, 0.50)           AS p50_altitude_m,
  1e2*count(device_lat)/count(*)                        AS latlong_coverage_pct
FROM tiled
GROUP BY 1,2,3,4
HAVING sum(w) > 100000
ORDER BY est_sessions DESC

================================================================================
-- QUERY: count28.sql   ->  volume counts
================================================================================

-- Ghana volume: rows + weighted sessions by day and RAT generation
-- table: cell_tower_info_v2:mobile
--
-- WARNING: `country` is NOT a partition key, so this scans the full global
-- table across the whole ds range (~224B rows/day, ~6.3T rows for 28 days)
-- to return ~5.5B. Correct but slow -- it will not finish inside the 120s
-- meta-CLI dispatch ceiling, which is why it belongs here in Daiquery.
--
-- GROUPING SETS returns three levels in one result:
--   (ds, generation) -> per-day per-generation
--   (generation)     -> 28-day total per generation   [ds IS NULL]
--   ()               -> 28-day grand total            [both NULL]
SELECT
  ds,
  generation,
  count(*)   AS raw_rows,
  sum(w)     AS est_sessions
FROM (
  SELECT
    ds,
    CAST(weight AS bigint) AS w,
    CASE
      WHEN connection_subtype = 'nrnsa'                                      THEN '5G-NSA'
      WHEN connection_subtype IN ('nr','5gnr','5g','nr-14')                  THEN '5G-SA'
      WHEN connection_subtype IN ('lte','lte_ca','4g')                       THEN '4G'
      WHEN connection_subtype IN ('umts','wcdma','hsdpa','hsupa','hspa',
           'hspa+','h','h+','dc_hspap','cdma - evdo rev. a',
           'cdma - evdo rev','cdma - evdo rev. 0')                           THEN '3G'
      WHEN connection_subtype IN ('gprs','edge','gsm','cdma - 1xrtt','cdma') THEN '2G'
      ELSE 'other'
    END AS generation
  FROM cell_tower_info_v2
  WHERE ds BETWEEN '2026-08-31' AND '2026-09-27'
    AND country = 'GH'
)
GROUP BY GROUPING SETS ((ds, generation), (generation), ())
ORDER BY ds NULLS LAST, generation NULLS LAST


================================================================================
ADDED 2026-09-30 — queries behind the cell, churn and 5G-capability deliverables.
Rebuilt from the session after its /tmp was lost; each passes `meta presto.query lint`.
Local pooling / joins for all of these: gh_postprocess.py.
================================================================================


================================================================================
-- QUERY: cube_cell.sql   ->  gh_cell_28d_tiled.csv
================================================================================

-- Ghana CELL-grain cube with centroids. Pool locally to any tile size you like.
-- Grain: op_mcc x op_mnc x ci x generation   (cells are small, so tiling them
-- locally into z=12 / z=14 quadkeys is accurate in a way TAC centroids are not)
SELECT
  op_mcc, op_mnc, ci, tac, generation,
  count(*)                                        AS raw_rows,
  sum(w)                                          AS est_sessions,
  approx_percentile(device_lat,  w, 0.50)         AS centroid_lat,
  approx_percentile(device_long, w, 0.50)         AS centroid_long,
  approx_percentile(device_lat,  w, 0.10)         AS lat_p10,
  approx_percentile(device_lat,  w, 0.90)         AS lat_p90,
  approx_percentile(device_long, w, 0.10)         AS long_p10,
  approx_percentile(device_long, w, 0.90)         AS long_p90,
  approx_percentile(device_acc,  w, 0.50)         AS p50_gps_acc_m,
  approx_percentile(rtt,  w, 0.50)                AS p50_rtt,
  approx_percentile(rtt,  w, 0.90)                AS p90_rtt,
  approx_percentile(ttfb, w, 0.50)                AS p50_ttfb,
  approx_percentile(ttlb, w, 0.50)                AS p50_ttlb,
  sum(tp *w)                                      AS pkts_w,
  sum(tpl*w)                                      AS pkts_lost_w,
  approx_percentile(signal_dbm, w, 0.50)          AS p50_signal_dbm,
  approx_percentile(rsrp_4g,  w, 0.50)            AS p50_rsrp_4g,
  approx_percentile(rsrq_4g,  w, 0.50)            AS p50_rsrq_4g,
  approx_percentile(ta_4g,    w, 0.50)            AS p50_ta_4g,
  approx_percentile(nr2_ss_rsrp, w, 0.50)         AS nr2_p50_ss_rsrp,
  approx_percentile(nr2_ss_sinr, w, 0.50)         AS nr2_p50_ss_sinr,
  count(nr2_ss_rsrp)                              AS n_nr2_ss_rsrp,
  approx_distinct(band)                           AS distinct_bands,
  approx_distinct(pci)                            AS distinct_pcis,
  1e2*sum(IF(status_code>=500,w,0))/nullif(sum(w),0) AS http5xx_pct,
  1e2*count(device_lat)/count(*)                  AS latlong_coverage_pct
FROM (
  SELECT
    CAST(weight AS bigint) AS w, device_lat, device_long, device_acc,
    rtt, ttfb, ttlb, status_code,
    IF(total_packets      < 0, NULL, total_packets)      AS tp,
    IF(total_packets_lost < 0, NULL, total_packets_lost) AS tpl,
    cell.mcc AS op_mcc, TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.ci AS ci, cell.tac AS tac, cell.pci AS pci,
    COALESCE(array_join(cell.bands,'+'),'unknown') AS band,
    IF(cell.signal_dbm = 2147483647, NULL, cell.signal_dbm) AS signal_dbm,
    IF(cell.rsrp       = 2147483647, NULL, cell.rsrp)       AS rsrp_4g,
    IF(cell.rsrq       = 2147483647, NULL, cell.rsrq)       AS rsrq_4g,
    IF(cell.timing_advance = 2147483647, NULL, cell.timing_advance) AS ta_4g,
    IF(element_at(nr_secondary_cell_info,1).ss_rsrp = 2147483647, NULL,
       element_at(nr_secondary_cell_info,1).ss_rsrp) AS nr2_ss_rsrp,
    IF(element_at(nr_secondary_cell_info,1).ss_sinr = 2147483647, NULL,
       element_at(nr_secondary_cell_info,1).ss_sinr) AS nr2_ss_sinr,
    CASE WHEN cell.network_type='nr'    THEN '5G'
         WHEN cell.network_type='lte'   THEN '4G'
         WHEN cell.network_type='wcdma' THEN '3G'
         WHEN cell.network_type='gsm'   THEN '2G'
         ELSE 'other' END AS generation
  FROM (
    SELECT *, COALESCE(element_at(filter(registered_cell_info, x -> x.connection_status=1),1),
                       element_at(registered_cell_info,1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>' AND country='GH'
  )
)
WHERE ci IS NOT NULL
GROUP BY 1,2,3,4,5
HAVING sum(w) > 20000


================================================================================
-- QUERY: sh_s1.sql   ->  gh_cell_28d_full (shard 1: latency)
================================================================================

SELECT
  ds, op_mcc, op_mnc, generation, tac, ci,
  count(*) AS raw_rows, sum(w) AS est_sessions,
  approx_percentile(rtt,  w, ARRAY[0.10,0.25,0.50,0.75,0.90,0.95,0.99]) AS rtt_q,
  approx_percentile(ttfb, w, ARRAY[0.50,0.90,0.95]) AS ttfb_q,
  approx_percentile(ttlb, w, ARRAY[0.50,0.90]) AS ttlb_q,
  approx_percentile(upstream_latency, w, ARRAY[0.50,0.90]) AS upstream_q,
  approx_percentile(upload_latency,   w, ARRAY[0.50,0.90]) AS upload_q,
  approx_percentile(rtt_variance,     w, ARRAY[0.50,0.90]) AS rtt_var_q
FROM (
  SELECT
    ds, CAST(weight AS bigint) AS w,
    rtt, ttfb, ttlb, upstream_latency, upload_latency, rtt_variance,
    cell.mcc AS op_mcc, TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.ci AS ci, cell.tac AS tac,
    CASE WHEN cell.network_type='nr'    THEN '5G'
         WHEN cell.network_type='lte'   THEN '4G'
         WHEN cell.network_type='wcdma' THEN '3G'
         WHEN cell.network_type='gsm'   THEN '2G'
         ELSE 'other' END AS generation
  FROM (
    SELECT *, COALESCE(element_at(filter(registered_cell_info, x -> x.connection_status=1),1),
                       element_at(registered_cell_info,1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>' AND country='GH'
  )
)
WHERE ci IS NOT NULL
GROUP BY 1,2,3,4,5,6
HAVING sum(w) > 20000


================================================================================
-- QUERY: sh_s2.sql   ->  gh_cell_28d_full (shard 2: 2G/3G/4G signal)
================================================================================

SELECT
  ds, op_mcc, op_mnc, generation, tac, ci,
  count(*) AS raw_rows, sum(w) AS est_sessions,
  approx_percentile(IF(cell.signal_dbm=2147483647,NULL,cell.signal_dbm), w, ARRAY[0.10,0.50,0.90]) AS signal_dbm_q,
  approx_percentile(IF(cell.rsrp=2147483647,NULL,cell.rsrp), w, ARRAY[0.10,0.50,0.90]) AS rsrp_4g_q,
  approx_percentile(IF(cell.rsrq=2147483647,NULL,cell.rsrq), w, ARRAY[0.50]) AS rsrq_4g_q,
  approx_percentile(IF(cell.rssnr=2147483647,NULL,cell.rssnr), w, ARRAY[0.50]) AS rssnr_4g_q,
  approx_percentile(IF(cell.rssi=2147483647,NULL,cell.rssi), w, ARRAY[0.10,0.50,0.90]) AS rssi_q,
  approx_percentile(IF(cell.timing_advance=2147483647,NULL,cell.timing_advance), w, ARRAY[0.50,0.90]) AS ta_4g_q,
  approx_percentile(IF(lte_sinr=2147483647,NULL,lte_sinr), w, ARRAY[0.50]) AS lte_sinr_q
FROM (
  SELECT
    ds, CAST(weight AS bigint) AS w,
    lte_sinr,
    cell.mcc AS op_mcc, TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.ci AS ci, cell.tac AS tac,
    cell,
    CASE WHEN cell.network_type='nr'    THEN '5G'
         WHEN cell.network_type='lte'   THEN '4G'
         WHEN cell.network_type='wcdma' THEN '3G'
         WHEN cell.network_type='gsm'   THEN '2G'
         ELSE 'other' END AS generation
  FROM (
    SELECT *, COALESCE(element_at(filter(registered_cell_info, x -> x.connection_status=1),1),
                       element_at(registered_cell_info,1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>' AND country='GH'
  )
)
WHERE ci IS NOT NULL
GROUP BY 1,2,3,4,5,6
HAVING sum(w) > 20000


================================================================================
-- QUERY: sh_s3.sql   ->  gh_cell_28d_full (shard 3: 5G + NR secondary)
================================================================================

SELECT
  ds, op_mcc, op_mnc, generation, tac, ci,
  count(*) AS raw_rows, sum(w) AS est_sessions,
  approx_percentile(IF(cell.ss_rsrp=2147483647,NULL,cell.ss_rsrp), w, ARRAY[0.50]) AS ss_rsrp_5g_q,
  approx_percentile(IF(cell.ss_rsrq=2147483647,NULL,cell.ss_rsrq), w, ARRAY[0.50]) AS ss_rsrq_5g_q,
  approx_percentile(IF(cell.ss_sinr=2147483647,NULL,cell.ss_sinr), w, ARRAY[0.50]) AS ss_sinr_5g_q,
  approx_percentile(IF(nr2.ss_rsrp=2147483647,NULL,nr2.ss_rsrp), w, ARRAY[0.10,0.50,0.90]) AS nr2_ss_rsrp_q,
  approx_percentile(IF(nr2.ss_rsrq=2147483647,NULL,nr2.ss_rsrq), w, ARRAY[0.50]) AS nr2_ss_rsrq_q,
  approx_percentile(IF(nr2.ss_sinr=2147483647,NULL,nr2.ss_sinr), w, ARRAY[0.50]) AS nr2_ss_sinr_q,
  count(nr2.ss_rsrp) AS n_nr2_ss_rsrp,
  1e2*sum(IF(n_nr_sec>0,w,0))/nullif(sum(w),0) AS pct_has_nr_secondary
FROM (
  SELECT
    ds, CAST(weight AS bigint) AS w,
    element_at(nr_secondary_cell_info,1) AS nr2,
    COALESCE(cardinality(nr_secondary_cell_info),0) AS n_nr_sec,
    cell.mcc AS op_mcc, TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.ci AS ci, cell.tac AS tac,
    cell,
    CASE WHEN cell.network_type='nr'    THEN '5G'
         WHEN cell.network_type='lte'   THEN '4G'
         WHEN cell.network_type='wcdma' THEN '3G'
         WHEN cell.network_type='gsm'   THEN '2G'
         ELSE 'other' END AS generation
  FROM (
    SELECT *, COALESCE(element_at(filter(registered_cell_info, x -> x.connection_status=1),1),
                       element_at(registered_cell_info,1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>' AND country='GH'
  )
)
WHERE ci IS NOT NULL
GROUP BY 1,2,3,4,5,6
HAVING sum(w) > 20000


================================================================================
-- QUERY: sh_s5.sql   ->  gh_cell_28d_full (shard 5: bytes/loss/outcomes/mix)
================================================================================

SELECT
  ds, op_mcc, op_mnc, generation, tac, ci,
  count(*) AS raw_rows, sum(w) AS est_sessions,
  approx_percentile(resp_bytes, w, ARRAY[0.50,0.90]) AS resp_bytes_q,
  approx_percentile(req_bytes,  w, ARRAY[0.50]) AS req_bytes_q,
  approx_percentile(resp_hdr,   w, ARRAY[0.50]) AS resp_hdr_q,
  approx_percentile(req_hdr,    w, ARRAY[0.50]) AS req_hdr_q,
  approx_percentile(congestion_state, w, ARRAY[0.50]) AS congestion_q,
  sum(tp*w) AS pkts_w, sum(tpl*w) AS pkts_lost_w,
  sum(stp*w) AS srv_pkts_w, sum(stpl*w) AS srv_pkts_lost_w,
  sum(resp_bytes*w) AS resp_bytes_w, sum(req_bytes*w) AS req_bytes_w,
  1e2*sum(tpl*w)/nullif(sum(tp*w),0) AS loss_pct,
  1e2*sum(stpl*w)/nullif(sum(stp*w),0) AS srv_loss_pct,
  1e2*sum(IF(request_status='done',w,0))/nullif(sum(w),0) AS ok_pct,
  1e2*sum(IF(status_code>=500,w,0))/nullif(sum(w),0) AS http5xx_pct,
  1e2*sum(IF(status_code BETWEEN 400 AND 499,w,0))/nullif(sum(w),0) AS http4xx_pct,
  1e2*sum(IF(tower_changed,w,0))/nullif(sum(w),0) AS tower_changed_pct,
  1e2*sum(IF(congestion_state>1,w,0))/nullif(sum(w),0) AS pct_congested,
  1e2*sum(IF(ip_version='6',w,0))/nullif(sum(w),0) AS pct_ipv6,
  1e2*sum(IF(http_stack LIKE '%mvfst%',w,0))/nullif(sum(w),0) AS pct_quic,
  1e2*sum(IF(os='iPhone OS',w,0))/nullif(sum(w),0) AS pct_ios,
  1e2*sum(IF(os='Android',w,0))/nullif(sum(w),0) AS pct_android,
  1e2*sum(IF(home_country <> country,w,0))/nullif(sum(w),0) AS pct_roaming_homecountry,
  1e2*sum(IF(is_nr_nsa_signal_strength,w,0))/nullif(sum(w),0) AS pct_nr_signal_reported
FROM (
  SELECT
    ds, CAST(weight AS bigint) AS w,
    response_body_compressed_size AS resp_bytes, request_body_size AS req_bytes,
    response_header_compressed_size AS resp_hdr, request_header_compressed_size AS req_hdr,
    congestion_state,
    request_status, status_code, tower_changed, ip_version, http_stack, os,
    home_country, country, is_nr_nsa_signal_strength,
    IF(total_packets<0,NULL,total_packets) AS tp,
    IF(total_packets_lost<0,NULL,total_packets_lost) AS tpl,
    IF(server_total_packets<0,NULL,server_total_packets) AS stp,
    IF(server_total_packets_lost<0,NULL,server_total_packets_lost) AS stpl,
    cell.mcc AS op_mcc, TRY(CAST(cell.mnc AS integer)) AS op_mnc,
    cell.ci AS ci, cell.tac AS tac,
    CASE WHEN cell.network_type='nr'    THEN '5G'
         WHEN cell.network_type='lte'   THEN '4G'
         WHEN cell.network_type='wcdma' THEN '3G'
         WHEN cell.network_type='gsm'   THEN '2G'
         ELSE 'other' END AS generation
  FROM (
    SELECT *, COALESCE(element_at(filter(registered_cell_info, x -> x.connection_status=1),1),
                       element_at(registered_cell_info,1)) AS cell
    FROM cell_tower_info_v2
    WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>' AND country='GH'
  )
)
WHERE ci IS NOT NULL
GROUP BY 1,2,3,4,5,6
HAVING sum(w) > 20000


================================================================================
-- QUERY: churn_cell.sql   ->  gh_cell_churn_quality.csv (namespace: mobile, table: cell_visitation_demographics)
================================================================================

-- Ghana cell-level churn + demographics from cell_visitation_demographics.
-- ecgi is 'MCC-MNC-TAC-CI' -> decomposes to the same keys as gh_cell_28d_full.
-- MNC normalised to integer (620-01 / 620-1 / 620-001 are all MTN).
-- Visit floor keeps the CLI result under its PHP memory limit (php_oom otherwise).
SELECT
  split_part(ecgi,'-',1)                                   AS op_mcc,
  TRY(CAST(split_part(ecgi,'-',2) AS integer))             AS op_mnc,
  split_part(ecgi,'-',3)                                   AS tac,
  split_part(ecgi,'-',4)                                   AS ci,
  count(*)                                                 AS visits,
  approx_distinct(uuid)                                    AS uniq_users,
  count_if(is_churner=1)                                   AS churners,
  1e2*count_if(is_churner=1)/count(*)                      AS churn_pct,
  count_if(churn_from_carrier_id IS NOT NULL
           AND churn_from_carrier_id <> -1)                AS churn_in_events,
  approx_distinct(carrier_id)                              AS carriers,
  approx_distinct(app_id)                                  AS app_ids,
  1e2*count_if(age_bracket IN (1,2))/count(*)              AS pct_age_young,
  1e2*count_if(age_bracket IN (3,4,5))/count(*)            AS pct_age_mid,
  1e2*count_if(age_bracket >= 6)/count(*)                  AS pct_age_older,
  approx_distinct(age_bracket)                             AS age_brackets
FROM cell_visitation_demographics
WHERE ds = '<DATEID>' AND ecgi LIKE '620-%'
GROUP BY 1,2,3,4
HAVING count(*) >= 50


================================================================================
-- QUERY: gh5g_tac.sql   ->  gh_5g_tac_28d.csv
================================================================================

-- Ghana handset 5G capability per TAC (Android only: iOS carries no cell info).
-- Join key: upper(split_part(gsmarena.device_model,'__',1)) = upper(device_model).
WITH dev AS (
  -- GSMArena handset capability, one row per normalised model key.
  -- price 0 = unknown in the source, so it is nulled before averaging across variants.
  SELECT upper(split_part(device_model,'__',1))                     AS k,
         bool_or(has_5g)                                            AS has_5g,
         avg(IF(est_price_eur > 0, est_price_eur, NULL))                    AS price_eur,
         avg(days_since_release_or_announcement_date)                       AS age_days
  FROM ads_gsm_arena_mobile_device_info_qrt_version:ad_metrics
  WHERE ds = '2026-09-30'
  GROUP BY 1
),
b AS (
  SELECT ds, CAST(weight AS bigint) AS w, upper(device_model) AS k,
    cell.mcc AS op_mcc, TRY(CAST(cell.mnc AS integer)) AS op_mnc, cell.tac AS tac,
    CASE
      WHEN COALESCE(cardinality(nr_secondary_cell_info),0) > 0
        OR override_network_type IN ('NR_NSA','NR_NSA_MMWAVE')
        OR connection_subtype = 'nrnsa'                                      THEN '5G-NSA'
      WHEN cell.network_type='nr'    OR connection_subtype IN ('nr','5gnr','5g','nr-14') THEN '5G-SA'
      WHEN cell.network_type='lte'   OR connection_subtype IN ('lte','lte_ca','4g')      THEN '4G'
      WHEN cell.network_type='wcdma' OR connection_subtype IN ('umts','wcdma','hsdpa','hsupa','hspa','hspa+','h','h+','dc_hspap') THEN '3G'
      WHEN cell.network_type='gsm'   OR connection_subtype IN ('gprs','edge','gsm')     THEN '2G'
      ELSE 'other' END AS generation
  FROM (SELECT *, COALESCE(element_at(filter(registered_cell_info, x -> x.connection_status=1),1),
                           element_at(registered_cell_info,1)) AS cell
        FROM cell_tower_info_v2
        WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>' AND country='GH')
  WHERE cell.tac IS NOT NULL
)
SELECT
  b.ds, b.op_mcc, b.op_mnc, b.generation, b.tac,
  count(*)                                   AS raw_rows,
  sum(b.w)                                   AS est_sessions,
  sum(IF(d.k IS NOT NULL, b.w, 0))           AS matched_w,
  sum(IF(d.has_5g, b.w, 0))                  AS capable_5g_w,
  sum(IF(d.has_5g AND b.generation IN ('2G','3G','4G'), b.w, 0)) AS capable_5g_not_on_5g_w,
  -- sums only, so days / half-days pool exactly
  sum(IF(d.price_eur IS NOT NULL, b.w, 0))                   AS priced_w,
  sum(d.price_eur * b.w)                                     AS price_x_w,
  sum(IF(d.price_eur <  100, b.w, 0))                        AS price_lt100_w,
  sum(IF(d.price_eur >= 100 AND d.price_eur < 200, b.w, 0))  AS price_100_200_w,
  sum(IF(d.price_eur >= 200 AND d.price_eur < 400, b.w, 0))  AS price_200_400_w,
  sum(IF(d.price_eur >= 400, b.w, 0))                        AS price_ge400_w,
  sum(IF(d.age_days IS NOT NULL, b.w, 0))                    AS aged_w,
  sum(d.age_days * b.w)                                      AS age_x_w
FROM b LEFT JOIN dev d ON b.k = d.k
GROUP BY 1,2,3,4,5


================================================================================
-- QUERY: gh5g_cell.sql   ->  gh_5g_cell_28d.csv
================================================================================

-- Ghana handset 5G capability per CELL (>20000 weighted sessions per cell-day) (Android only: iOS carries no cell info).
-- Join key: upper(split_part(gsmarena.device_model,'__',1)) = upper(device_model).
WITH dev AS (
  -- GSMArena handset capability, one row per normalised model key.
  -- price 0 = unknown in the source, so it is nulled before averaging across variants.
  SELECT upper(split_part(device_model,'__',1))                     AS k,
         bool_or(has_5g)                                            AS has_5g,
         avg(IF(est_price_eur > 0, est_price_eur, NULL))                    AS price_eur,
         avg(days_since_release_or_announcement_date)                       AS age_days
  FROM ads_gsm_arena_mobile_device_info_qrt_version:ad_metrics
  WHERE ds = '2026-09-30'
  GROUP BY 1
),
b AS (
  SELECT ds, CAST(weight AS bigint) AS w, upper(device_model) AS k,
    cell.mcc AS op_mcc, TRY(CAST(cell.mnc AS integer)) AS op_mnc, cell.tac AS tac, cell.ci AS ci,
    CASE
      WHEN COALESCE(cardinality(nr_secondary_cell_info),0) > 0
        OR override_network_type IN ('NR_NSA','NR_NSA_MMWAVE')
        OR connection_subtype = 'nrnsa'                                      THEN '5G-NSA'
      WHEN cell.network_type='nr'    OR connection_subtype IN ('nr','5gnr','5g','nr-14') THEN '5G-SA'
      WHEN cell.network_type='lte'   OR connection_subtype IN ('lte','lte_ca','4g')      THEN '4G'
      WHEN cell.network_type='wcdma' OR connection_subtype IN ('umts','wcdma','hsdpa','hsupa','hspa','hspa+','h','h+','dc_hspap') THEN '3G'
      WHEN cell.network_type='gsm'   OR connection_subtype IN ('gprs','edge','gsm')     THEN '2G'
      ELSE 'other' END AS generation
  FROM (SELECT *, COALESCE(element_at(filter(registered_cell_info, x -> x.connection_status=1),1),
                           element_at(registered_cell_info,1)) AS cell
        FROM cell_tower_info_v2
        WHERE ds BETWEEN '<DATEID-27>' AND '<DATEID>' AND country='GH')
  WHERE cell.ci IS NOT NULL
)
SELECT
  b.ds, b.op_mcc, b.op_mnc, b.generation, b.tac, b.ci,
  count(*)                                   AS raw_rows,
  sum(b.w)                                   AS est_sessions,
  sum(IF(d.k IS NOT NULL, b.w, 0))           AS matched_w,
  sum(IF(d.has_5g, b.w, 0))                  AS capable_5g_w,
  sum(IF(d.has_5g AND b.generation IN ('2G','3G','4G'), b.w, 0)) AS capable_5g_not_on_5g_w,
  -- sums only, so days / half-days pool exactly
  sum(IF(d.price_eur IS NOT NULL, b.w, 0))                   AS priced_w,
  sum(d.price_eur * b.w)                                     AS price_x_w,
  sum(IF(d.price_eur <  100, b.w, 0))                        AS price_lt100_w,
  sum(IF(d.price_eur >= 100 AND d.price_eur < 200, b.w, 0))  AS price_100_200_w,
  sum(IF(d.price_eur >= 200 AND d.price_eur < 400, b.w, 0))  AS price_200_400_w,
  sum(IF(d.price_eur >= 400, b.w, 0))                        AS price_ge400_w,
  sum(IF(d.age_days IS NOT NULL, b.w, 0))                    AS aged_w,
  sum(d.age_days * b.w)                                      AS age_x_w
FROM b LEFT JOIN dev d ON b.k = d.k
GROUP BY 1,2,3,4,5,6
HAVING sum(b.w) > 20000
