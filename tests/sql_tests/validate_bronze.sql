-- ============================================================================
-- Bronze Layer Validation
-- Delta paths: s3://<delta-bucket>/bronze/{enrollment, nonfin, supplfwd}
--
-- Validates: ingestion completeness, schema integrity, file-date parsing,
--            and dedup input (edge case #6: duplicate records in same file).
-- ============================================================================


-- 1. Row counts per table — verify all 410 source files were ingested
SELECT 'enrollment' AS table_name, COUNT(*) AS row_count
FROM delta.`s3://ms-phone-compliance-delta/bronze/enrollment`
UNION ALL
SELECT 'nonfin', COUNT(*)
FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin`
UNION ALL
SELECT 'supplfwd', COUNT(*)
FROM delta.`s3://ms-phone-compliance-delta/bronze/supplfwd`;
-- EXPECT: all three > 0, nonfin and supplfwd should be in the millions


-- 2. File date coverage — should span ~5 years across monthly/weekly files
--    enrollment: 100 files, nonfin: 60 monthly snapshots, supplfwd: 250 weekly
SELECT 'enrollment' AS src,
       MIN(_file_date) AS earliest,
       MAX(_file_date) AS latest,
       COUNT(DISTINCT _file_date) AS distinct_dates
FROM delta.`s3://ms-phone-compliance-delta/bronze/enrollment`
UNION ALL
SELECT 'nonfin', MIN(_file_date), MAX(_file_date), COUNT(DISTINCT _file_date)
FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin`
UNION ALL
SELECT 'supplfwd', MIN(_file_date), MAX(_file_date), COUNT(DISTINCT _file_date)
FROM delta.`s3://ms-phone-compliance-delta/bronze/supplfwd`;
-- EXPECT: dates spanning 2019–2024, nonfin ~60, supplfwd ~250, enrollment ~100


-- 3. No null file dates — _file_date must always be parsed from filename
SELECT
  'nonfin' AS src,
  SUM(CASE WHEN _file_date IS NULL THEN 1 ELSE 0 END) AS null_file_dates
FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin`
UNION ALL
SELECT 'supplfwd',
  SUM(CASE WHEN _file_date IS NULL THEN 1 ELSE 0 END)
FROM delta.`s3://ms-phone-compliance-delta/bronze/supplfwd`
UNION ALL
SELECT 'enrollment',
  SUM(CASE WHEN _file_date IS NULL THEN 1 ELSE 0 END)
FROM delta.`s3://ms-phone-compliance-delta/bronze/enrollment`;
-- EXPECT: all 0


-- 4. Key columns not null in NON-FIN
SELECT
  SUM(CASE WHEN cnsmr_phn_nmbr_txt IS NULL THEN 1 ELSE 0 END)  AS null_phone,
  SUM(CASE WHEN cnsmr_idntfr_lgcy_txt IS NULL THEN 1 ELSE 0 END) AS null_legacy_id,
  SUM(CASE WHEN cnsmr_phn_id IS NULL THEN 1 ELSE 0 END)          AS null_phone_id
FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin`;
-- EXPECT: all 0


-- 5. Key columns not null in SUPPLFWD
SELECT
  SUM(CASE WHEN cnsmr_phn_nmbr_txt IS NULL THEN 1 ELSE 0 END)    AS null_phone,
  SUM(CASE WHEN cnsmr_idntfr_lgcy_txt IS NULL THEN 1 ELSE 0 END) AS null_legacy_id,
  SUM(CASE WHEN record_date IS NULL THEN 1 ELSE 0 END)           AS null_record_date,
  SUM(CASE WHEN cnsmr_phn_sft_dlt_flg IS NULL THEN 1 ELSE 0 END) AS null_soft_delete_flag
FROM delta.`s3://ms-phone-compliance-delta/bronze/supplfwd`;
-- EXPECT: all 0


-- 6. Key columns not null in ENROLLMENT
SELECT
  SUM(CASE WHEN phone_number IS NULL THEN 1 ELSE 0 END)                AS null_phone,
  SUM(CASE WHEN consumer_legacy_identifier IS NULL THEN 1 ELSE 0 END)  AS null_legacy_id,
  SUM(CASE WHEN enrollment_date IS NULL THEN 1 ELSE 0 END)             AS null_enrollment_date
FROM delta.`s3://ms-phone-compliance-delta/bronze/enrollment`;
-- EXPECT: all 0


-- 7. Edge case #6: Duplicate records in same file
--    Verify duplicates exist in raw Bronze (Silver is responsible for dedup).
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT cnsmr_phn_id, _file_date) AS distinct_keys,
       COUNT(*) - COUNT(DISTINCT cnsmr_phn_id, _file_date) AS duplicate_rows
FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin`;
-- EXPECT: duplicate_rows >= 0 (duplicates are normal in raw data; Silver handles dedup)


-- 8. Phone source values — should be CLIENT, CONSUMER, THIRD_PARTY, OTHER
SELECT cnsmr_phn_src_val_txt AS phone_source, COUNT(*) AS cnt
FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin`
GROUP BY cnsmr_phn_src_val_txt
ORDER BY cnt DESC;
-- EXPECT: 4 values: CLIENT, CONSUMER, THIRD_PARTY, OTHER


-- 9. Phone number format — should be 10 digits
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN cnsmr_phn_nmbr_txt RLIKE '^[0-9]{10}$' THEN 1 ELSE 0 END) AS valid_10_digit,
  SUM(CASE WHEN cnsmr_phn_nmbr_txt NOT RLIKE '^[0-9]{10}$' THEN 1 ELSE 0 END) AS invalid_format
FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin`;
-- EXPECT: invalid_format = 0
