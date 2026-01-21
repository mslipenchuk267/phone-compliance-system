#!/usr/bin/env python3
"""
Phone Consent Data Generator

Generates realistic fake data files for a coding challenge.
Target: ~1GB of total data across NON-FIN-BDL, SUPPLFWD, and ENRLMT files.

Usage:
    python generate_bank_data.py --output-dir ./data --target-size-gb 1.0
"""

import argparse
import csv
import gzip
import os
import random
import string
import uuid
from datetime import datetime, timedelta
from typing import Generator, List, Tuple, Optional
from dataclasses import dataclass, field
import json


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class GeneratorConfig:
    """Configuration for data generation."""

    num_accounts: int = 500_000
    phones_per_account_range: Tuple[int, int] = (1, 4)

    # Probability distributions
    consent_yes_probability: float = 0.05
    consent_no_probability: float = 0.05
    # Remaining probability = null/unknown consent

    soft_delete_probability: float = 0.08
    hard_delete_probability: float = 0.05
    consent_withdrawal_probability: float = 0.06

    # Date ranges
    start_date: datetime = field(default_factory=lambda: datetime(2019, 1, 1))
    end_date: datetime = field(default_factory=lambda: datetime(2024, 12, 31))

    # File generation settings
    num_nonfin_snapshots: int = 60  # Monthly snapshots over 5 years
    num_supplfwd_files: int = 250  # ~Weekly change files
    num_enrollment_files: int = 100  # Enrollment batches

    # Phone number settings
    area_codes: List[str] = field(
        default_factory=lambda: [
            "212",
            "213",
            "312",
            "404",
            "415",
            "469",
            "512",
            "617",
            "702",
            "713",
            "718",
            "747",
            "786",
            "818",
            "832",
            "917",
            "929",
            "949",
            "954",
            "972",
        ]
    )

    phone_types: List[str] = field(
        default_factory=lambda: ["CELL", "HOME", "WORK", "OTHER"]
    )
    phone_statuses: List[str] = field(
        default_factory=lambda: ["ACTIVE", "INACTIVE", "DISCONNECTED"]
    )
    phone_sources: List[str] = field(
        default_factory=lambda: ["CLIENT", "CONSUMER", "THIRD_PARTY", "OTHER"]
    )
    phone_technologies: List[str] = field(
        default_factory=lambda: ["WIRELESS", "LANDLINE", "VOIP"]
    )


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class Phone:
    """Represents a phone number associated with an account."""

    phone_id: str
    phone_number: str
    phone_type: str
    phone_status: str
    phone_source: str
    phone_technology: str
    quality_score: int
    consent_flag: Optional[str]  # 'Y', 'N', or None
    consent_date: Optional[str]
    created_date: datetime
    soft_deleted: bool = False
    soft_delete_date: Optional[datetime] = None
    hard_deleted: bool = False
    hard_delete_date: Optional[datetime] = None
    consent_withdrawn: bool = False
    consent_withdrawn_date: Optional[datetime] = None


@dataclass
class Account:
    """Represents a customer account."""

    account_number: str
    legacy_identifier: str
    agency_id: str
    ssn: str
    first_name: str
    last_name: str
    enrollment_date: datetime
    phones: List[Phone] = field(default_factory=list)


# =============================================================================
# Data Generation Functions
# =============================================================================


class DataGenerator:
    """Generates fake client data."""

    def __init__(self, config: GeneratorConfig, seed: int = 42):
        self.config = config
        random.seed(seed)
        self.first_names = self._load_names("first")
        self.last_names = self._load_names("last")

    def _load_names(self, name_type: str) -> List[str]:
        """Load or generate name lists."""
        # Common US names for realistic data
        first_names = [
            "James",
            "Mary",
            "John",
            "Patricia",
            "Robert",
            "Jennifer",
            "Michael",
            "Linda",
            "William",
            "Elizabeth",
            "David",
            "Barbara",
            "Richard",
            "Susan",
            "Joseph",
            "Jessica",
            "Thomas",
            "Sarah",
            "Charles",
            "Karen",
            "Christopher",
            "Nancy",
            "Daniel",
            "Lisa",
            "Matthew",
            "Betty",
            "Anthony",
            "Margaret",
            "Mark",
            "Sandra",
            "Donald",
            "Ashley",
            "Steven",
            "Kimberly",
            "Paul",
            "Emily",
            "Andrew",
            "Donna",
            "Joshua",
            "Michelle",
            "Kenneth",
            "Dorothy",
            "Kevin",
            "Carol",
            "Brian",
            "Amanda",
            "George",
            "Melissa",
            "Edward",
            "Deborah",
        ]
        last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Rodriguez",
            "Martinez",
            "Hernandez",
            "Lopez",
            "Gonzalez",
            "Wilson",
            "Anderson",
            "Thomas",
            "Taylor",
            "Moore",
            "Jackson",
            "Martin",
            "Lee",
            "Perez",
            "Thompson",
            "White",
            "Harris",
            "Sanchez",
            "Clark",
            "Ramirez",
            "Lewis",
            "Robinson",
            "Walker",
            "Young",
            "Allen",
            "King",
            "Wright",
            "Scott",
            "Torres",
            "Nguyen",
            "Hill",
            "Flores",
            "Green",
            "Adams",
            "Nelson",
            "Baker",
            "Hall",
            "Rivera",
            "Campbell",
            "Mitchell",
        ]
        return first_names if name_type == "first" else last_names

    def _generate_phone_number(self) -> str:
        """Generate a realistic US phone number."""
        area_code = random.choice(self.config.area_codes)
        exchange = str(random.randint(200, 999))
        subscriber = str(random.randint(1000, 9999))
        return f"{area_code}{exchange}{subscriber}"

    def _generate_ssn(self) -> str:
        """Generate a fake SSN (marked as fake with 9xx prefix)."""
        # Use 900-999 range which is not valid for real SSNs
        return f"9{random.randint(10, 99)}{random.randint(10, 99)}{random.randint(1000, 9999)}"

    def _generate_account_number(self) -> str:
        """Generate a 16-digit account number."""
        return "".join([str(random.randint(0, 9)) for _ in range(16)])

    def _random_date(self, start: datetime, end: datetime) -> datetime:
        """Generate a random date between start and end."""
        delta = end - start
        random_days = random.randint(0, delta.days)
        return start + timedelta(days=random_days)

    def _generate_consent(self) -> Tuple[Optional[str], Optional[str]]:
        """Generate consent flag and date."""
        roll = random.random()
        if roll < self.config.consent_yes_probability:
            consent_flag = "Y"
        elif (
            roll
            < self.config.consent_yes_probability + self.config.consent_no_probability
        ):
            consent_flag = "N"
        else:
            return None, None

        consent_date = self._random_date(self.config.start_date, self.config.end_date)
        return consent_flag, consent_date.strftime("%Y-%m-%d")

    def generate_phone(self, created_date: datetime) -> Phone:
        """Generate a single phone record."""
        consent_flag, consent_date = self._generate_consent()

        phone = Phone(
            phone_id=str(uuid.uuid4()),
            phone_number=self._generate_phone_number(),
            phone_type=random.choice(self.config.phone_types),
            phone_status=random.choice(self.config.phone_statuses),
            phone_source=random.choice(self.config.phone_sources),
            phone_technology=random.choice(self.config.phone_technologies),
            quality_score=random.randint(1, 100),
            consent_flag=consent_flag,
            consent_date=consent_date,
            created_date=created_date,
        )

        # Determine if this phone gets soft-deleted, hard-deleted, or consent withdrawn
        if random.random() < self.config.soft_delete_probability:
            phone.soft_deleted = True
            phone.soft_delete_date = self._random_date(
                created_date, self.config.end_date
            )
        elif random.random() < self.config.hard_delete_probability:
            phone.hard_deleted = True
            phone.hard_delete_date = self._random_date(
                created_date, self.config.end_date
            )

        # Consent can be withdrawn even if not deleted
        if (
            phone.consent_flag == "Y"
            and random.random() < self.config.consent_withdrawal_probability
        ):
            phone.consent_withdrawn = True
            phone.consent_withdrawn_date = self._random_date(
                datetime.strptime(consent_date, "%Y-%m-%d")
                if consent_date
                else created_date,
                self.config.end_date,
            )

        return phone

    def generate_account(self) -> Account:
        """Generate a single account with phones."""
        enrollment_date = self._random_date(self.config.start_date, self.config.end_date)
        account_number = self._generate_account_number()

        account = Account(
            account_number=account_number,
            legacy_identifier=account_number + "P",  # P = Primary consumer
            agency_id=f"AGN{random.randint(100000, 999999)}",
            ssn=self._generate_ssn(),
            first_name=random.choice(self.first_names),
            last_name=random.choice(self.last_names),
            enrollment_date=enrollment_date,
        )

        # Generate phones for this account
        num_phones = random.randint(*self.config.phones_per_account_range)
        for _ in range(num_phones):
            phone = self.generate_phone(enrollment_date)
            account.phones.append(phone)

        return account

    def generate_accounts(self) -> Generator[Account, None, None]:
        """Generate all accounts."""
        for i in range(self.config.num_accounts):
            if i % 50000 == 0:
                print(f"  Generated {i:,} / {self.config.num_accounts:,} accounts...")
            yield self.generate_account()


# =============================================================================
# File Writers
# =============================================================================


class FileWriter:
    """Writes generated data to files in client format."""

    def __init__(self, output_dir: str, config: GeneratorConfig):
        self.output_dir = output_dir
        self.config = config
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, "MAINTENANCE"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "ENROLLMENT"), exist_ok=True)

    def _generate_file_timestamp(self, base_date: datetime) -> str:
        """Generate a file timestamp in client format."""
        return base_date.strftime("%Y%m%d%H%M%S") + str(random.randint(100, 999))

    def write_nonfin_csv(
        self, accounts: List[Account], file_date: datetime, file_index: int
    ):
        """Write a NON-FIN-BDL file (snapshot of current state)."""
        timestamp = self._generate_file_timestamp(file_date)
        filename = f"SN_SVC_DMBDL_DataManager-{timestamp}-NON-FIN-BDL_EXPORT_{file_index:03d}.csv.gz"
        filepath = os.path.join(self.output_dir, "MAINTENANCE", filename)

        with gzip.open(filepath, "wt", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="|")

            # Header
            writer.writerow(
                [
                    "cnsmr_phn_id",
                    "cnsmr_idntfr_lgcy_txt",
                    "cnsmr_idntfr_agncy_id",
                    "cnsmr_idntfr_ssn_txt",
                    "cnsmr_nm_frst_txt",
                    "cnsmr_nm_lst_txt",
                    "cnsmr_phn_nmbr_txt",
                    "cnsmr_phn_typ_val_txt",
                    "cnsmr_phn_stts_val_txt",
                    "cnsmr_phn_src_val_txt",
                    "cnsmr_phn_tchnlgy_typ_val_txt",
                    "cnsmr_phn_qlty_score_nmbr",
                    "cnsmr_phn_cnsnt_flg",
                    "cnsmr_phn_cnsnt_dt",
                ]
            )

            for account in accounts:
                for phone in account.phones:
                    # Skip if hard-deleted before this file date
                    if (
                        phone.hard_deleted
                        and phone.hard_delete_date
                        and phone.hard_delete_date < file_date
                    ):
                        continue
                    # Skip if soft-deleted (NON-FIN doesn't show soft-deleted)
                    if (
                        phone.soft_deleted
                        and phone.soft_delete_date
                        and phone.soft_delete_date < file_date
                    ):
                        continue

                    # Determine current consent status
                    consent_flag = phone.consent_flag
                    consent_date = phone.consent_date
                    if (
                        phone.consent_withdrawn
                        and phone.consent_withdrawn_date
                        and phone.consent_withdrawn_date < file_date
                    ):
                        consent_flag = "N"
                        consent_date = phone.consent_withdrawn_date.strftime("%Y-%m-%d")

                    writer.writerow(
                        [
                            phone.phone_id,
                            account.legacy_identifier,
                            account.agency_id,
                            account.ssn,
                            account.first_name,
                            account.last_name,
                            phone.phone_number,
                            phone.phone_type,
                            phone.phone_status,
                            phone.phone_source,
                            phone.phone_technology,
                            phone.quality_score,
                            consent_flag or "",
                            consent_date or "",
                        ]
                    )

        return filepath

    def write_supplfwd_csv(
        self, accounts: List[Account], file_date: datetime, file_index: int
    ):
        """Write a SUPPLFWD file (change records with soft-delete flags)."""
        timestamp = self._generate_file_timestamp(file_date)
        filename = f"SN_SVC_DMBDL_DataManager-{timestamp}-SUPPLFWD-BDL_EXPORT_{file_index:03d}.csv.gz"
        filepath = os.path.join(self.output_dir, "MAINTENANCE", filename)

        with gzip.open(filepath, "wt", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="|")

            # Header
            writer.writerow(
                [
                    "cnsmr_phn_hst_id",
                    "cnsmr_idntfr_lgcy_txt",
                    "cnsmr_idntfr_agncy_id",
                    "cnsmr_idntfr_ssn_txt",
                    "cnsmr_nm_frst_txt",
                    "cnsmr_nm_lst_txt",
                    "cnsmr_phn_nmbr_txt",
                    "cnsmr_phn_typ_val_txt",
                    "cnsmr_phn_stts_val_txt",
                    "cnsmr_phn_src_val_txt",
                    "cnsmr_phn_tchnlgy_typ_val_txt",
                    "cnsmr_phn_qlty_score_nmbr",
                    "cnsmr_phn_cnsnt_flg",
                    "cnsmr_phn_cnsnt_dt",
                    "cnsmr_phn_sft_dlt_flg",
                    "record_date",
                ]
            )

            # Only include records that have changes around this file date
            window_start = file_date - timedelta(days=7)
            window_end = file_date

            for account in accounts:
                for phone in account.phones:
                    records_to_write = []

                    # Check for soft-delete events in window
                    if phone.soft_deleted and phone.soft_delete_date:
                        if window_start <= phone.soft_delete_date <= window_end:
                            records_to_write.append((phone.soft_delete_date, "Y"))

                    # Check for consent withdrawal events in window
                    if phone.consent_withdrawn and phone.consent_withdrawn_date:
                        if window_start <= phone.consent_withdrawn_date <= window_end:
                            records_to_write.append((phone.consent_withdrawn_date, "N"))

                    # Also include some regular updates (phone status changes, etc.)
                    if random.random() < 0.02:  # 2% of phones get random updates
                        records_to_write.append((file_date, "N"))

                    for record_date, soft_delete_flag in records_to_write:
                        consent_flag = phone.consent_flag
                        consent_date = phone.consent_date

                        # If this is a consent withdrawal record
                        if (
                            phone.consent_withdrawn
                            and phone.consent_withdrawn_date == record_date
                        ):
                            consent_flag = "N"
                            consent_date = record_date.strftime("%Y-%m-%d")

                        writer.writerow(
                            [
                                str(uuid.uuid4()),  # History record gets new ID
                                account.legacy_identifier,
                                account.agency_id,
                                account.ssn,
                                account.first_name,
                                account.last_name,
                                phone.phone_number,
                                phone.phone_type,
                                phone.phone_status,
                                phone.phone_source,
                                phone.phone_technology,
                                phone.quality_score,
                                consent_flag or "",
                                consent_date or "",
                                soft_delete_flag,
                                record_date.strftime("%Y-%m-%d"),
                            ]
                        )

        return filepath

    def write_enrollment_csv(
        self, accounts: List[Account], file_date: datetime, file_index: int
    ):
        """Write an ENRLMT file (enrollment records)."""
        timestamp = self._generate_file_timestamp(file_date)
        filename = f"IC_A2RGMRHG_DMENRL_DataManager-{timestamp}-ENRLMT-EXPORT_{file_index:03d}.csv.gz"
        filepath = os.path.join(self.output_dir, "ENROLLMENT", filename)

        with gzip.open(filepath, "wt", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="|")

            # Header
            writer.writerow(
                [
                    "account_number",
                    "consumer_legacy_identifier",
                    "agency_id",
                    "ssn",
                    "first_name",
                    "last_name",
                    "phone_number",
                    "phone_type",
                    "phone_technology",
                    "phone_status",
                    "phone_source",
                    "quality_score",
                    "enrollment_date",
                ]
            )

            # Filter to accounts enrolled around this date
            window_start = file_date - timedelta(days=7)
            window_end = file_date

            for account in accounts:
                if window_start <= account.enrollment_date <= window_end:
                    for phone in account.phones:
                        writer.writerow(
                            [
                                account.account_number,
                                account.legacy_identifier,
                                account.agency_id,
                                account.ssn,
                                account.first_name,
                                account.last_name,
                                phone.phone_number,
                                phone.phone_type,
                                phone.phone_technology,
                                phone.phone_status,
                                phone.phone_source,
                                phone.quality_score,
                                account.enrollment_date.strftime("%Y-%m-%d"),
                            ]
                        )

        return filepath


# =============================================================================
# Main Generation Logic
# =============================================================================


def generate_dataset(output_dir: str, target_size_gb: float = 1.0, seed: int = 42):
    """Generate the complete dataset."""

    # Adjust config based on target size
    # Empirical: 500K accounts with 2-3 phones each generates ~3.5GB compressed
    # Scale accordingly
    scale_factor = target_size_gb / 3.5

    config = GeneratorConfig(
        num_accounts=int(500_000 * scale_factor),
    )

    print(f"=" * 60)
    print(f"Phone Consent Data Generator")
    print(f"=" * 60)
    print(f"Target size: {target_size_gb} GB")
    print(f"Number of accounts: {config.num_accounts:,}")
    print(f"Output directory: {output_dir}")
    print()

    # Generate accounts
    print("Step 1: Generating accounts...")
    generator = DataGenerator(config, seed=seed)
    accounts = list(generator.generate_accounts())
    print(
        f"  Generated {len(accounts):,} accounts with {sum(len(a.phones) for a in accounts):,} total phones"
    )
    print()

    # Write files
    writer = FileWriter(output_dir, config)

    # Generate date ranges for files
    date_range = (config.end_date - config.start_date).days

    # NON-FIN-BDL files (monthly snapshots)
    print(f"Step 2: Writing {config.num_nonfin_snapshots} NON-FIN-BDL files...")
    nonfin_files = []
    for i in range(config.num_nonfin_snapshots):
        file_date = config.start_date + timedelta(
            days=int(date_range * i / config.num_nonfin_snapshots)
        )
        filepath = writer.write_nonfin_csv(accounts, file_date, i)
        nonfin_files.append(filepath)
        if (i + 1) % 10 == 0:
            print(f"  Written {i + 1} / {config.num_nonfin_snapshots} files...")
    print()

    # SUPPLFWD files (weekly change files)
    print(f"Step 3: Writing {config.num_supplfwd_files} SUPPLFWD files...")
    supplfwd_files = []
    for i in range(config.num_supplfwd_files):
        file_date = config.start_date + timedelta(
            days=int(date_range * i / config.num_supplfwd_files)
        )
        filepath = writer.write_supplfwd_csv(accounts, file_date, i)
        supplfwd_files.append(filepath)
        if (i + 1) % 50 == 0:
            print(f"  Written {i + 1} / {config.num_supplfwd_files} files...")
    print()

    # ENRLMT files
    print(f"Step 4: Writing {config.num_enrollment_files} ENRLMT files...")
    enrollment_files = []
    for i in range(config.num_enrollment_files):
        file_date = config.start_date + timedelta(
            days=int(date_range * i / config.num_enrollment_files)
        )
        filepath = writer.write_enrollment_csv(accounts, file_date, i)
        enrollment_files.append(filepath)
        if (i + 1) % 20 == 0:
            print(f"  Written {i + 1} / {config.num_enrollment_files} files...")
    print()

    # Calculate total size
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(output_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)

    print(f"=" * 60)
    print(f"Generation Complete!")
    print(f"=" * 60)
    print(
        f"Total files generated: {len(nonfin_files) + len(supplfwd_files) + len(enrollment_files)}"
    )
    print(f"  - NON-FIN-BDL: {len(nonfin_files)}")
    print(f"  - SUPPLFWD: {len(supplfwd_files)}")
    print(f"  - ENRLMT: {len(enrollment_files)}")
    print(f"Total compressed size: {total_size / (1024**3):.2f} GB")
    print(f"Estimated uncompressed size: {total_size * 5 / (1024**3):.2f} GB")
    print()

    # Write manifest
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "config": {
            "num_accounts": config.num_accounts,
            "date_range": f"{config.start_date.date()} to {config.end_date.date()}",
            "consent_yes_probability": config.consent_yes_probability,
            "soft_delete_probability": config.soft_delete_probability,
            "consent_withdrawal_probability": config.consent_withdrawal_probability,
        },
        "files": {
            "nonfin": len(nonfin_files),
            "supplfwd": len(supplfwd_files),
            "enrollment": len(enrollment_files),
        },
        "total_size_bytes": total_size,
    }

    manifest_path = os.path.join(output_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Manifest written to: {manifest_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate phone consent test data"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./client_data",
        help="Output directory for generated files",
    )
    parser.add_argument(
        "--target-size-gb",
        type=float,
        default=1.0,
        help="Target total size in GB (compressed)",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility"
    )

    args = parser.parse_args()
    generate_dataset(args.output_dir, args.target_size_gb, args.seed)


if __name__ == "__main__":
    main()
