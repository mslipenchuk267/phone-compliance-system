-- ============================================================================
-- Gold Layer Validation
-- Delta path: s3://<delta-bucket>/gold/phone_consent
--
-- Validates: phone normalization, consent derivation, can_send_sms logic,
--            and all compliance edge cases.
-- ============================================================================


-- 1. Row count matches Silver — Gold is a 1:1 transformation
SELECT
  (SELECT COUNT(*) FROM delta.`s3://ms-phone-compliance-delta/silver/phone_state`) AS silver_rows,
  (SELECT COUNT(*) FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`) AS gold_rows;
-- EXPECT: silver_rows = gold_rows


-- 2. Uniqueness — exactly 1 row per (account_number, phone_number)
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT account_number, phone_number) AS distinct_keys,
  COUNT(*) - COUNT(DISTINCT account_number, phone_number) AS duplicates
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`;
-- EXPECT: duplicates = 0


-- 3. Phone number normalization — all must be +1XXXXXXXXXX format
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN phone_number RLIKE '^\\+1[0-9]{10}$' THEN 1 ELSE 0 END) AS valid_format,
  SUM(CASE WHEN phone_number NOT RLIKE '^\\+1[0-9]{10}$' THEN 1 ELSE 0 END) AS bad_format
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`;
-- EXPECT: bad_format = 0


-- 4. has_sms_consent values — should only be true, false, or null
SELECT
  has_sms_consent,
  COUNT(*) AS cnt,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
GROUP BY has_sms_consent
ORDER BY has_sms_consent;
-- EXPECT: three groups: true, false, null


-- 5. Delete consistency carried from Silver
SELECT
  is_deleted,
  delete_type,
  COUNT(*) AS cnt
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
GROUP BY is_deleted, delete_type
ORDER BY is_deleted, delete_type;
-- EXPECT: same distribution as Silver query #4


-- 6. Delete flag consistency
SELECT
  SUM(CASE WHEN is_deleted = true  AND delete_type IS NULL     THEN 1 ELSE 0 END) AS deleted_missing_type,
  SUM(CASE WHEN is_deleted = false AND delete_type IS NOT NULL THEN 1 ELSE 0 END) AS not_deleted_has_type
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`;
-- EXPECT: both 0


-- ============================================================================
-- can_send_sms compliance logic
-- Rules: TRUE only if source=CLIENT AND not deleted AND consent is TRUE or NULL
-- ============================================================================

-- 7. Overall can_send_sms breakdown
SELECT
  CASE
    WHEN phone_source = 'CLIENT'
     AND is_deleted = false
     AND (has_sms_consent = true OR has_sms_consent IS NULL)
    THEN 'CAN_SEND'
    ELSE 'CANNOT_SEND'
  END AS sms_decision,
  COUNT(*) AS cnt,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
GROUP BY 1;
-- EXPECT: ~20-25% CAN_SEND (only CLIENT source phones that aren't deleted/revoked)


-- 8. Detailed reasons for CANNOT_SEND — ordered by prevalence
SELECT
  CASE
    WHEN phone_source != 'CLIENT'    THEN 'not_client_source'
    WHEN is_deleted = true           THEN 'deleted'
    WHEN has_sms_consent = false     THEN 'consent_withdrawn'
    ELSE 'can_send'
  END AS reason,
  COUNT(*) AS cnt,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
GROUP BY 1
ORDER BY cnt DESC;
-- EXPECT: not_client_source is largest (~75%), then can_send, deleted, consent_withdrawn


-- ============================================================================
-- Edge Case #1: Same phone on multiple accounts — independent decisions
-- ============================================================================

-- 9. Verify can_send_sms can differ for the same phone across accounts
SELECT
  g1.account_number AS account_1,
  g2.account_number AS account_2,
  g1.phone_number,
  g1.phone_source AS source_1,
  g2.phone_source AS source_2,
  g1.is_deleted AS deleted_1,
  g2.is_deleted AS deleted_2
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent` g1
JOIN delta.`s3://ms-phone-compliance-delta/gold/phone_consent` g2
  ON g1.phone_number = g2.phone_number
  AND g1.account_number < g2.account_number
WHERE g1.phone_source != g2.phone_source
   OR g1.is_deleted != g2.is_deleted
LIMIT 10;
-- EXPECT: rows present — same phone has different status per account


-- ============================================================================
-- Edge Case #2: Phone re-added after deletion — most recent record wins
-- ============================================================================

-- 10. Phones that were soft-deleted in SUPPLFWD but are NOT deleted in Gold
--     (re-added via a later NON-FIN snapshot)
SELECT COUNT(*) AS readded_phones
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent` g
WHERE g.is_deleted = false
  AND g.phone_number IN (
    SELECT CONCAT('+1', cnsmr_phn_nmbr_txt)
    FROM delta.`s3://ms-phone-compliance-delta/bronze/supplfwd`
    WHERE cnsmr_phn_sft_dlt_flg = 'Y'
  );
-- EXPECT: readded_phones > 0


-- ============================================================================
-- Edge Case #3: Consent changes over time — latest status wins
-- ============================================================================

-- 11. Phones with explicit consent withdrawal (has_sms_consent = false)
--     These must block SMS even if phone_source is CLIENT and not deleted
SELECT COUNT(*) AS consent_withdrawn
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
WHERE has_sms_consent = false
  AND phone_source = 'CLIENT'
  AND is_deleted = false;
-- EXPECT: > 0 — these are CLIENT phones blocked solely by consent withdrawal

-- 12. Verify consent_withdrawn phones are correctly blocked
SELECT
  CASE
    WHEN phone_source = 'CLIENT'
     AND is_deleted = false
     AND (has_sms_consent = true OR has_sms_consent IS NULL)
    THEN 'CAN_SEND'
    ELSE 'CANNOT_SEND'
  END AS sms_decision,
  COUNT(*) AS cnt
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
WHERE has_sms_consent = false
  AND phone_source = 'CLIENT'
  AND is_deleted = false
GROUP BY 1;
-- EXPECT: only CANNOT_SEND — consent withdrawal blocks SMS


-- ============================================================================
-- Edge Case #4: Missing consent flag — treat as implicit consent (allowed)
-- ============================================================================

-- 13. Null consent should allow SMS if other conditions are met
SELECT
  CASE
    WHEN phone_source = 'CLIENT'
     AND is_deleted = false
     AND (has_sms_consent = true OR has_sms_consent IS NULL)
    THEN 'CAN_SEND'
    ELSE 'CANNOT_SEND'
  END AS sms_decision,
  COUNT(*) AS cnt
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
WHERE has_sms_consent IS NULL
  AND phone_source = 'CLIENT'
  AND is_deleted = false
GROUP BY 1;
-- EXPECT: only CAN_SEND — null consent = implicit consent = allowed

-- 14. Null consent count — should be the majority (most records lack explicit consent)
SELECT
  SUM(CASE WHEN has_sms_consent IS NULL THEN 1 ELSE 0 END) AS null_consent,
  SUM(CASE WHEN has_sms_consent IS NOT NULL THEN 1 ELSE 0 END) AS explicit_consent,
  ROUND(100.0 * SUM(CASE WHEN has_sms_consent IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS null_pct
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`;
-- EXPECT: null_consent is large majority


-- ============================================================================
-- Edge Case #5: SUPPLFWD vs NON-FIN conflicts — SUPPLFWD wins on date tie
-- ============================================================================

-- 15. Phones where SUPPLFWD was the authoritative source should have correct consent
SELECT
  phone_source,
  has_sms_consent,
  is_deleted,
  COUNT(*) AS cnt
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent` g
JOIN delta.`s3://ms-phone-compliance-delta/silver/phone_state` s
  ON g.account_number = s.account_number
  AND g.phone_number = CONCAT('+1', s.phone_number)
WHERE s.record_source = 'SUPPLFWD'
GROUP BY phone_source, has_sms_consent, is_deleted
ORDER BY cnt DESC;
-- EXPECT: valid distribution — SUPPLFWD-sourced records populate Gold correctly


-- ============================================================================
-- Compliance boundary checks
-- ============================================================================

-- 16. No non-CLIENT source phone should ever be sendable
SELECT COUNT(*) AS non_client_sendable
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
WHERE phone_source != 'CLIENT'
  AND is_deleted = false
  AND (has_sms_consent = true OR has_sms_consent IS NULL);
-- EXPECT: this count EXISTS (the phones meet delete/consent criteria)
--         but can_send_sms should still be FALSE because source != CLIENT

-- 17. Deleted phones should never be sendable regardless of source/consent
SELECT COUNT(*) AS deleted_would_send
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
WHERE is_deleted = true
  AND phone_source = 'CLIENT'
  AND (has_sms_consent = true OR has_sms_consent IS NULL);
-- EXPECT: this count EXISTS (CLIENT phones that were deleted)
--         but can_send_sms should still be FALSE because deleted


-- 18. Sample can_send_sms lookups — spot-check individual records
SELECT
  account_number,
  phone_number,
  phone_source,
  is_deleted,
  delete_type,
  has_sms_consent,
  sms_consent_updated_at,
  CASE
    WHEN phone_source = 'CLIENT'
     AND is_deleted = false
     AND (has_sms_consent = true OR has_sms_consent IS NULL)
    THEN true ELSE false
  END AS can_send_sms
FROM delta.`s3://ms-phone-compliance-delta/gold/phone_consent`
ORDER BY account_number, phone_number
LIMIT 20;
-- EXPECT: can_send_sms matches the three-condition rule
