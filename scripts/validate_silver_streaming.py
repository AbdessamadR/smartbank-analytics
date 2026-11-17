
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    TimestampType,
    BooleanType,
)

BASE = "/opt/spark/data/lakehouse"

BRONZE_PATH = f"{BASE}/bronze_streaming/transactions"
SILVER_PATH = f"{BASE}/silver_streaming/transactions"
QUARANTINE_PATH = f"{BASE}/quarantine_streaming/transactions"
ACCOUNTS_PATH = f"{BASE}/silver/accounts"

EXPECTED_TYPES = {
    "tran_id": IntegerType,
    "orig_acct": IntegerType,
    "bene_acct": IntegerType,
    "base_amt": DoubleType,
    "tran_timestamp": TimestampType,
    "is_sar": BooleanType,
    "alert_id": IntegerType,
}

REQUIRED_COLUMNS = list(EXPECTED_TYPES.keys()) + ["tx_type"]


def count_rows(df):
    return df.count()


def main():
    spark = (
        SparkSession.builder
        .appName("SmartBank-Silver-Streaming-Validation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze = spark.read.format("delta").load(BRONZE_PATH)
        silver = spark.read.format("delta").load(SILVER_PATH)
        quarantine = (
            spark.read.format("delta").load(QUARANTINE_PATH)
        )
        accounts = (
            spark.read.format("delta")
            .load(ACCOUNTS_PATH)
            .select("acct_id")
            .filter(F.col("acct_id").isNotNull())
            .distinct()
        )

        bronze_count = count_rows(bronze)
        silver_count = count_rows(silver)
        quarantine_count = count_rows(quarantine)
        account_count = count_rows(accounts)

        # 1. Schema validation
        actual_types = {
            field.name: type(field.dataType)
            for field in silver.schema.fields
        }

        missing_columns = [
            column for column in REQUIRED_COLUMNS
            if column not in actual_types
        ]

        type_errors = {
            column: actual_types.get(column).__name__
            for column, expected in EXPECTED_TYPES.items()
            if column not in actual_types
            or actual_types[column] != expected
        }

        schema_ok = (
            len(missing_columns) == 0
            and len(type_errors) == 0
        )

        # 2. Business key validation
        null_tran_ids = (
            silver.filter(F.col("tran_id").isNull()).count()
        )

        duplicate_groups = (
            silver.groupBy("tran_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        distinct_tran_ids = (
            silver.select("tran_id").distinct().count()
        )

        duplicate_rows = silver_count - distinct_tran_ids

        # 3. Required fields
        null_required = {}

        for column in REQUIRED_COLUMNS:
            null_required[column] = (
                silver.filter(F.col(column).isNull()).count()
            )

        null_required_total = sum(null_required.values())

        # 4. Business rules
        negative_amounts = (
            silver.filter(F.col("base_amt") < 0).count()
        )

        empty_transaction_types = (
            silver.filter(
                F.col("tx_type").isNull()
                | (F.trim(F.col("tx_type")) == "")
            ).count()
        )

        # 5. Referential integrity
        invalid_orig_accounts = (
            silver.alias("s")
            .join(
                accounts.alias("a"),
                F.col("s.orig_acct") == F.col("a.acct_id"),
                "left_anti"
            )
            .count()
        )

        invalid_beneficiary_accounts = (
            silver.alias("s")
            .join(
                accounts.alias("a"),
                F.col("s.bene_acct") == F.col("a.acct_id"),
                "left_anti"
            )
            .count()
        )

        # 6. Source and quarantine reconciliation
        source_duplicate_groups = (
            bronze.groupBy("tran_id")
            .count()
            .filter(
                F.col("tran_id").isNotNull()
                & (F.col("count") > 1)
            )
            .count()
        )

        reconciliation_ok = (
            bronze_count == silver_count + quarantine_count
        )

        # 7. Final validation
        checks = {
            "SCHEMA_VALID": schema_ok,
            "NO_NULL_BUSINESS_KEYS": null_tran_ids == 0,
            "NO_DUPLICATE_SILVER_KEYS": duplicate_groups == 0,
            "NO_NULL_REQUIRED_FIELDS": null_required_total == 0,
            "NO_NEGATIVE_AMOUNTS": negative_amounts == 0,
            "NO_EMPTY_TX_TYPES": empty_transaction_types == 0,
            "ORIGIN_ACCOUNTS_VALID": invalid_orig_accounts == 0,
            "BENEFICIARY_ACCOUNTS_VALID": (
                invalid_beneficiary_accounts == 0
            ),
            "SOURCE_RECONCILIATION": reconciliation_ok,
        }

        print("\n========== SILVER STREAMING AUDIT ==========")
        print("BRONZE_ROWS:", bronze_count)
        print("SILVER_ROWS:", silver_count)
        print("QUARANTINE_ROWS:", quarantine_count)
        print("REFERENCE_ACCOUNTS:", account_count)

        print("\n--- Schema ---")
        print("MISSING_COLUMNS:", missing_columns)
        print("TYPE_ERRORS:", type_errors)

        print("\n--- Uniqueness ---")
        print("NULL_TRAN_IDS:", null_tran_ids)
        print("DISTINCT_TRAN_IDS:", distinct_tran_ids)
        print("DUPLICATE_KEY_GROUPS:", duplicate_groups)
        print("DUPLICATE_ROWS:", duplicate_rows)
        print("SOURCE_DUPLICATE_KEY_GROUPS:", source_duplicate_groups)

        print("\n--- Required fields ---")
        print("NULL_REQUIRED_FIELDS:", null_required)

        print("\n--- Business rules ---")
        print("NEGATIVE_AMOUNTS:", negative_amounts)
        print("EMPTY_TX_TYPES:", empty_transaction_types)

        print("\n--- Referential integrity ---")
        print("INVALID_ORIG_ACCOUNTS:", invalid_orig_accounts)
        print(
            "INVALID_BENEFICIARY_ACCOUNTS:",
            invalid_beneficiary_accounts
        )

        print("\n--- Reconciliation ---")
        print(
            "SOURCE_EQUALS_SILVER_PLUS_QUARANTINE:",
            reconciliation_ok
        )

        print("\n--- Validation checks ---")

        for name, passed in checks.items():
            print(f"{name}: {'PASS' if passed else 'FAIL'}")

        overall = all(checks.values())

        print("\nSILVER_STREAMING_AUDIT_SUCCESS:", overall)

        if not overall:
            raise RuntimeError(
                "Silver Streaming quality audit failed"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
