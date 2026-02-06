# Data Engineering Challenge: Client Phone Consent System

## Overview

You are building a **phone consent compliance system** for a financial services company. The system must determine whether SMS outreach is permitted for a given phone number on a specific customer account.

**The core business rule is simple:**
> We can only send SMS messages to phone numbers that were provided by the client (not sourced from third parties), AND where the customer has not withdrawn their consent.

However, implementing this rule requires processing millions of records across multiple file types spanning 5 years of historical data.

---

## Background

Your client, a major financial institution, sends you three types of files:

### 1. NON-FIN-BDL Files (Account Snapshots)
- **Purpose**: Monthly snapshots showing the current phones associated with each account
- **Key characteristic**: Phones that disappear from newer files have been "hard deleted"
- **Location**: `MAINTENANCE/` directory
- **Format**: Pipe-delimited CSV (gzipped)

### 2. SUPPLFWD Files (Change History)
- **Purpose**: Records of phone changes including explicit soft-delete flags
- **Key characteristic**: Contains `cnsmr_phn_sft_dlt_flg` column ('Y' = deleted)
- **Location**: `MAINTENANCE/` directory
- **Format**: Pipe-delimited CSV (gzipped)

### 3. ENRLMT Files (Enrollments)
- **Purpose**: Initial account enrollments with associated phones
- **Key characteristic**: The original source of account-phone associations
- **Location**: `ENROLLMENT/` directory
- **Format**: Pipe-delimited CSV (gzipped)

---

## Data Schema

### NON-FIN-BDL Schema
| Column | Description |
|--------|-------------|
| `cnsmr_phn_id` | Unique phone record ID |
| `cnsmr_idntfr_lgcy_txt` | Legacy identifier (account_number + consumer type) |
| `cnsmr_idntfr_agncy_id` | Agency ID |
| `cnsmr_idntfr_ssn_txt` | SSN (masked in test data) |
| `cnsmr_nm_frst_txt` | First name |
| `cnsmr_nm_lst_txt` | Last name |
| `cnsmr_phn_nmbr_txt` | Phone number (10 digits) |
| `cnsmr_phn_typ_val_txt` | Phone type (CELL, HOME, WORK, OTHER) |
| `cnsmr_phn_stts_val_txt` | Phone status (ACTIVE, INACTIVE, DISCONNECTED) |
| `cnsmr_phn_src_val_txt` | Phone source (CLIENT, CONSUMER, THIRD_PARTY, OTHER) |
| `cnsmr_phn_tchnlgy_typ_val_txt` | Technology (WIRELESS, LANDLINE, VOIP) |
| `cnsmr_phn_qlty_score_nmbr` | Quality score (1-100) |
| `cnsmr_phn_cnsnt_flg` | SMS consent flag ('Y', 'N', or empty) |
| `cnsmr_phn_cnsnt_dt` | Consent date (YYYY-MM-DD) |

### SUPPLFWD Schema
Same as NON-FIN-BDL, plus:
| Column | Description |
|--------|-------------|
| `cnsmr_phn_sft_dlt_flg` | Soft delete flag ('Y' = deleted, 'N' = active) |
| `record_date` | Date of this change record |

### ENRLMT Schema
| Column | Description |
|--------|-------------|
| `account_number` | 16-digit account number |
| `consumer_legacy_identifier` | Legacy identifier |
| `agency_id` | Agency ID |
| `ssn` | SSN (masked) |
| `first_name` | First name |
| `last_name` | Last name |
| `phone_number` | Phone number |
| `phone_type` | Phone type |
| `phone_technology` | Technology type |
| `phone_status` | Status |
| `phone_source` | Source of phone |
| `quality_score` | Quality score |
| `enrollment_date` | Date of enrollment |

---

## Your Task

Build a data pipeline that:

### Part 1: Data Ingestion & Processing (Bronze → Silver)

1. **Ingest all files** into a queryable format (Athena/Redshift/Spark)
2. **Reconcile the data** to determine the current state of each phone:
   - Detect hard deletes (phone in older NON-FIN but missing from newer NON-FIN)
   - Apply soft deletes from SUPPLFWD records
   - Track consent withdrawals (consent_flag changing from 'Y' to 'N')

### Part 2: Consent Derivation (Silver → Gold)

Create a final `phone_consent` table with one row per `(account_number, phone_number)` containing:

| Column | Description |
|--------|-------------|
| `account_number` | The account |
| `phone_number` | The phone (normalized to +1XXXXXXXXXX format) |
| `phone_source` | Where the phone came from |
| `has_sms_consent` | TRUE, FALSE, or NULL (unknown) |
| `sms_consent_updated_at` | When consent was last updated |
| `is_deleted` | Whether the phone has been deleted |
| `delete_type` | 'hard_delete', 'soft_delete', or NULL |
| `deleted_at` | When it was deleted |

### Part 3: Compliance API

Implement a function/query that answers:

```
can_send_sms(account_number, phone_number) → boolean
```

**Rules:**
- Return `TRUE` only if:
  - The phone was provided by the client (`phone_source` = 'CLIENT')
  - The phone is NOT deleted
  - `has_sms_consent` is TRUE or NULL (implicit consent allowed)
- Return `FALSE` otherwise

---

## Technical Requirements

### Infrastructure
- Use AWS services (S3, Athena, Redshift, Glue, or Lambda)
- OR use Spark/Databricks
- Provide Infrastructure-as-Code (Terraform)

### Code Quality
- Modular, testable code
- Unit tests for core logic
- Documentation

### Performance
- Must handle the full dataset (1GB+ compressed, 5GB+ uncompressed)
- Queries should return in < 5 seconds

---

## Deliverables

1. **Infrastructure code** to set up the data platform
2. **ETL pipeline code** (Bronze → Silver → Gold)
3. **SQL queries** or code for the compliance check
4. **Test suite** demonstrating correctness
5. **README** with:
   - Architecture diagram
   - Setup instructions
   - Design decisions and tradeoffs

---

## Evaluation Criteria

| Criteria | Weight |
|----------|--------|
| **Correctness** - Does the logic properly handle all edge cases? | 30% |
| **Code Quality** - Clean, readable, maintainable code | 25% |
| **Architecture** - Appropriate use of AWS/data platform services | 20% |
| **Performance** - Efficient processing of large datasets | 15% |
| **Documentation** - Clear explanation of approach | 10% |

---

## Edge Cases to Consider

1. **Same phone on multiple accounts** - Each account-phone pair is independent
2. **Phone re-added after deletion** - Most recent record wins
3. **Consent changes over time** - Track the latest consent status
4. **Missing consent flag** - Treat as implicit consent (allowed)
5. **SUPPLFWD vs NON-FIN conflicts** - SUPPLFWD is authoritative if more recent
6. **Duplicate records in same file** - Deduplicate by phone_id

## Interviewer submission notes
I'm looking for runnable code and infrastructure as code, as I will deploy your solution onto our cloud infra. Please provide a public github repository that we can review.

it should contain
A terraform file that stands up cloud infrastructure, with configurable s3 bucket names (i.e. with variables.tf)

A makefile: 
 - that will allow me to push any data files 
 - that will allow be to build and push any docker images to a configurable registry (i.e. with variables.tf)

---

## Test Data

Generate test data using the provided script:

```bash
python generate_bank_data.py --output-dir ./data --target-size-gb 1.0
```

This generates:
- ~60 NON-FIN-BDL monthly snapshot files
- ~250 SUPPLFWD weekly change files
- ~100 ENRLMT enrollment files
- ~500K accounts with 1-4 phones each

---

## Bonus Challenges

1. **Incremental Processing** - Design for daily updates, not just batch
2. **Data Quality Monitoring** - Add checks for anomalies
3. **Audit Trail** - Track all changes for compliance reporting
4. **Real-time API** - Sub-100ms consent lookups

---

## Questions?

If you have questions about the requirements, document your assumptions in your README.

Good luck!
