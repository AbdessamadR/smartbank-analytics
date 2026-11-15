
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE = "/opt/spark/data/lakehouse"


def load_table(spark, table):
    return (
        spark.read.format("delta")
        .load(f"{BASE}/silver/{table}")
    )


def find_invalid(df, checks):
    """
    Detect rows with missing foreign-key references.
    checks: (source_column, reference_df, reference_column, alias, reason)
    """
    joined = df.alias("src")
    reasons = []

    for source_col, reference_df, reference_col, alias, reason in checks:
        ref = (
            reference_df
            .select(F.col(reference_col).alias(f"_{alias}_ref"))
            .distinct()
            .alias(alias)
        )

        joined = joined.join(
            ref,
            F.col(f"src.{source_col}") == F.col(f"{alias}._{alias}_ref"),
            "left"
        )

        reasons.append(
            F.when(
                F.col(f"{alias}._{alias}_ref").isNull(),
                F.lit(reason)
            )
        )

    return (
        joined
        .withColumn("_rejection_reason", F.concat_ws("|", *reasons))
        .filter(F.col("_rejection_reason") != "")
        .select("src.*", "_rejection_reason")
    )


def quarantine_and_update(table, df, invalid, keys):
    total = df.count()

    invalid = invalid.dropDuplicates(keys)
    invalid_count = invalid.count()

    if invalid_count == 0:
        print(f"{table}: {total} rows, no rejected records")
        return df

    invalid = (
        invalid
        .withColumn("_source_table", F.lit(table))
        .withColumn("_rejected_at", F.current_timestamp())
    )

    # Preserve rejected records in Delta quarantine.
    quarantine_path = f"{BASE}/quarantine/{table}"

    (
        invalid.write
        .format("delta")
        .mode("append")
        .save(quarantine_path)
    )

    # Remove rejected business keys from Silver.
    valid = df.join(
        invalid.select(*keys).dropDuplicates(),
        on=keys,
        how="left_anti"
    )

    valid_count = valid.count()

    if total != valid_count + invalid_count:
        raise RuntimeError(
            f"Count reconciliation failed for {table}"
        )

    (
        valid.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{BASE}/silver/{table}")
    )

    print(
        f"{table}: total={total}, "
        f"valid={valid_count}, quarantined={invalid_count}"
    )

    return valid


def main(spark):
    accounts = load_table(spark, "accounts")
    transactions = load_table(spark, "transactions")

    # 1. Transactions -> Accounts
    invalid_transactions = find_invalid(
        transactions,
        [
            (
                "orig_acct", accounts, "acct_id",
                "origin", "UNKNOWN_ORIG_ACCT"
            ),
            (
                "bene_acct", accounts, "acct_id",
                "beneficiary", "UNKNOWN_BENE_ACCT"
            ),
        ]
    )

    valid_transactions = quarantine_and_update(
        "transactions",
        transactions,
        invalid_transactions,
        ["tran_id"]
    )

    # 2. Alert transactions -> Transactions and Accounts
    alert_transactions = load_table(spark, "alert_transactions")

    invalid_alert_transactions = find_invalid(
        alert_transactions,
        [
            (
                "tran_id", valid_transactions, "tran_id",
                "transaction", "UNKNOWN_TRAN_ID"
            ),
            (
                "orig_acct", accounts, "acct_id",
                "origin", "UNKNOWN_ORIG_ACCT"
            ),
            (
                "bene_acct", accounts, "acct_id",
                "beneficiary", "UNKNOWN_BENE_ACCT"
            ),
        ]
    )

    quarantine_and_update(
        "alert_transactions",
        alert_transactions,
        invalid_alert_transactions,
        ["alert_id", "tran_id"]
    )

    # 3. Alert accounts -> Accounts
    alert_accounts = load_table(spark, "alert_accounts")

    invalid_alert_accounts = find_invalid(
        alert_accounts,
        [
            (
                "acct_id", accounts, "acct_id",
                "account", "UNKNOWN_ACCT_ID"
            ),
        ]
    )

    quarantine_and_update(
        "alert_accounts",
        alert_accounts,
        invalid_alert_accounts,
        ["alert_id", "acct_id"]
    )

    # 4. SAR accounts -> Accounts
    sar_accounts = load_table(spark, "sar_accounts")

    invalid_sar_accounts = find_invalid(
        sar_accounts,
        [
            (
                "account_id", accounts, "acct_id",
                "account", "UNKNOWN_ACCOUNT_ID"
            ),
        ]
    )

    quarantine_and_update(
        "sar_accounts",
        sar_accounts,
        invalid_sar_accounts,
        ["alert_id", "account_id"]
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("SmartBankReferentialIntegrity")
        .getOrCreate()
    )

    try:
        main(spark)
        print("REFERENTIAL_INTEGRITY_SUCCESS: True")
    finally:
        spark.stop()
