-- ============================================================================
-- Silver Layer Validation
-- Delta path: s3://<delta-bucket>/silver/phone_state
--
-- Validates: dedup, hard/soft delete detection, merge logic, enrollment-only
--            phones, and all edge cases from reconciliation.
-- ============================================================================


-- 1. Row count and uniqueness — exactly 1 row per (account_number, phone_number)
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT account_number, phone_number) AS distinct_keys,
  COUNT(*) - COUNT(DISTINCT account_number, phone_number) AS duplicates
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`;
-- EXPECT: duplicates = 0


-- 2. Account number format — 16 numeric digits (legacy_id minus trailing char)
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN LENGTH(account_number) = 16 AND account_number RLIKE '^[0-9]+$'
      THEN 1 ELSE 0 END) AS valid_format,
  SUM(CASE WHEN LENGTH(account_number) != 16 OR account_number NOT RLIKE '^[0-9]+$'
      THEN 1 ELSE 0 END) AS bad_format
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`;
-- EXPECT: bad_format = 0


-- 3. Delete flag consistency — is_deleted must always match delete_type
SELECT
  SUM(CASE WHEN is_deleted = true  AND delete_type IS NULL     THEN 1 ELSE 0 END) AS deleted_missing_type,
  SUM(CASE WHEN is_deleted = false AND delete_type IS NOT NULL THEN 1 ELSE 0 END) AS not_deleted_has_type,
  SUM(CASE WHEN deleted_at IS NOT NULL AND is_deleted = false  THEN 1 ELSE 0 END) AS has_date_not_deleted
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`;
-- EXPECT: all 0


-- 4. Delete type breakdown
SELECT
  is_deleted,
  delete_type,
  COUNT(*) AS cnt
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
GROUP BY is_deleted, delete_type
ORDER BY is_deleted, delete_type;
-- EXPECT: three groups: (false, NULL), (true, hard_delete), (true, soft_delete)


-- 5. Record source distribution — expect NON-FIN, SUPPLFWD, and ENRLMT
SELECT
  record_source,
  COUNT(*) AS cnt,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
GROUP BY record_source
ORDER BY cnt DESC;
-- EXPECT: NON-FIN majority, some SUPPLFWD, small number ENRLMT-only


-- 6. Phone source distribution
SELECT
  phone_source,
  COUNT(*) AS cnt,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
GROUP BY phone_source
ORDER BY cnt DESC;
-- EXPECT: roughly even split among CLIENT, CONSUMER, THIRD_PARTY, OTHER (~25% each)


-- ============================================================================
-- Edge Case #1: Same phone on multiple accounts
-- Each (account_number, phone_number) pair is independent — the same phone_number
-- can appear with different accounts and have different statuses.
-- ============================================================================

-- 7. Find phones shared across multiple accounts
SELECT phone_number, COUNT(DISTINCT account_number) AS num_accounts
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
GROUP BY phone_number
HAVING COUNT(DISTINCT account_number) > 1
ORDER BY num_accounts DESC
LIMIT 10;
-- EXPECT: rows present — confirms shared phones exist and are tracked independently

-- 8. Verify independent status per account for a shared phone
SELECT account_number, phone_number, is_deleted, delete_type, consent_flag, phone_source
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
WHERE phone_number IN (
  SELECT phone_number
  FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
  GROUP BY phone_number
  HAVING COUNT(DISTINCT account_number) > 1
  LIMIT 1
)
ORDER BY account_number;
-- EXPECT: same phone_number, different account_numbers, potentially different statuses


-- ============================================================================
-- Edge Case #2: Phone re-added after deletion — most recent record wins
-- ============================================================================

-- 9. Re-added phones: soft-deleted in SUPPLFWD but latest NON-FIN is more recent
--    These should NOT be marked as deleted (NON-FIN re-add overrides soft delete).
SELECT
  COUNT(*) AS readded_count
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
WHERE record_source = 'NON-FIN'
  AND is_deleted = false
  AND phone_number IN (
    -- phones that had a soft delete at some point
    SELECT cnsmr_phn_nmbr_txt
    FROM delta.`s3://ms-phone-compliance-delta/bronze/supplfwd`
    WHERE cnsmr_phn_sft_dlt_flg = 'Y'
  );
-- EXPECT: readded_count > 0 — confirms re-adds are properly handled


-- ============================================================================
-- Edge Case #3: Consent changes over time — latest consent status wins
-- ============================================================================

-- 10. Consent flag distribution in Silver
SELECT
  consent_flag,
  COUNT(*) AS cnt
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
GROUP BY consent_flag
ORDER BY cnt DESC;
-- EXPECT: mix of Y, N, and NULL values

-- 11. Consent came from the latest record (not an older one)
--     Verify non-null consent dates are reasonable
SELECT
  MIN(consent_date) AS earliest_consent,
  MAX(consent_date) AS latest_consent,
  COUNT(*) AS records_with_consent_date
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
WHERE consent_date IS NOT NULL;
-- EXPECT: dates within the data range


-- ============================================================================
-- Edge Case #5: SUPPLFWD vs NON-FIN conflicts — SUPPLFWD wins on date tie
-- ============================================================================

-- 12. Records where SUPPLFWD won the merge
SELECT
  COUNT(*) AS supplfwd_wins
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
WHERE record_source = 'SUPPLFWD';
-- EXPECT: > 0, confirms SUPPLFWD records are present as winning source

-- 13. Verify SUPPLFWD-sourced records have valid fields
SELECT
  SUM(CASE WHEN phone_source IS NULL THEN 1 ELSE 0 END) AS null_source,
  SUM(CASE WHEN phone_number IS NULL THEN 1 ELSE 0 END) AS null_phone,
  SUM(CASE WHEN event_date IS NULL THEN 1 ELSE 0 END)   AS null_event_date
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
WHERE record_source = 'SUPPLFWD';
-- EXPECT: all 0


-- ============================================================================
-- Edge Case #6: Duplicate records in same file — dedup by phone_id
-- ============================================================================

-- 14. Silver should have fewer rows than raw Bronze NON-FIN (dedup + merge)
SELECT
  (SELECT COUNT(*) FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin`) AS bronze_nonfin_rows,
  (SELECT COUNT(*) FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`) AS silver_rows;
-- EXPECT: silver_rows << bronze_nonfin_rows (many snapshots collapsed to latest state)


-- ============================================================================
-- Hard delete detection
-- ============================================================================

-- 15. Hard deletes: deleted_at should be before the latest NON-FIN snapshot
SELECT
  MIN(deleted_at) AS earliest_hard_delete,
  MAX(deleted_at) AS latest_hard_delete,
  COUNT(*) AS hard_delete_count
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
WHERE delete_type = 'hard_delete';
-- EXPECT: latest_hard_delete < max NON-FIN file_date (phones missing from latest)

-- 16. Hard-deleted phones should NOT appear in the latest NON-FIN snapshot
SELECT COUNT(*) AS hard_deleted_in_latest
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state` s
WHERE s.delete_type = 'hard_delete'
  AND EXISTS (
    SELECT 1
    FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin` n
    WHERE n.cnsmr_idntfr_lgcy_txt = s.legacy_identifier
      AND n.cnsmr_phn_nmbr_txt = s.phone_number
      AND n._file_date = (SELECT MAX(_file_date) FROM delta.`s3://ms-phone-compliance-delta/bronze/nonfin`)
  );
-- EXPECT: 0 — hard-deleted means absent from latest snapshot


-- ============================================================================
-- Soft delete detection
-- ============================================================================

-- 17. Soft-deleted records — should have SUPPLFWD as record source
--     (unless a later NON-FIN re-add overrode the soft delete)
SELECT
  record_source,
  COUNT(*) AS cnt
FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`
WHERE delete_type = 'soft_delete'
GROUP BY record_source;
-- EXPECT: primarily SUPPLFWD

-- 18. Soft delete flag Y in SUPPLFWD should correspond to soft deletes in Silver
SELECT
  COUNT(DISTINCT s.cnsmr_idntfr_lgcy_txt, s.cnsmr_phn_nmbr_txt) AS supplfwd_soft_deletes
FROM delta.`s3://ms-phone-compliance-delta/bronze/supplfwd` s
WHERE s.cnsmr_phn_sft_dlt_flg = 'Y';
-- EXPECT: > 0, these feed into Silver soft_delete detection
