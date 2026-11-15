import great_expectations as gx
from pyspark.sql import SparkSession

RULES = {
    "transactions": (["tran_id"], "base_amt"),
    "accounts": (["acct_id"], None),
    "alert_transactions": (["alert_id", "tran_id"], "base_amt"),
    "alert_accounts": (["alert_id", "acct_id"], None),
    "sar_accounts": (["alert_id", "account_id"], None),
}

spark = SparkSession.builder.appName("SmartBankSilverQuality").getOrCreate()
try:
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_spark(name="smartbank_silver_quality")
    outcomes = {}

    for table, (keys, amount_column) in RULES.items():
        df = spark.read.format("delta").load(
            f"/opt/spark/data/lakehouse/silver/{table}"
        )
        suite = gx.ExpectationSuite(name=f"{table}_silver_suite")
        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
        )

        for key in keys:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=key)
            )

        if len(keys) == 1:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeUnique(column=keys[0])
            )
        else:
            suite.add_expectation(
                gx.expectations.ExpectCompoundColumnsToBeUnique(
                    column_list=keys
                )
            )

        if amount_column:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=amount_column
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=amount_column, min_value=0
                )
            )

        asset = datasource.add_dataframe_asset(name=f"{table}_asset")
        batch_definition = asset.add_batch_definition_whole_dataframe(
            f"{table}_batch"
        )
        context.suites.add(suite)
        validation = gx.ValidationDefinition(
            name=f"{table}_silver_validation",
            data=batch_definition,
            suite=suite,
        )
        validation = context.validation_definitions.add(validation)
        result = validation.run(batch_parameters={"dataframe": df})

        outcomes[table] = result.success
        print(f"\nTABLE: {table}")
        print(f"SUCCESS: {result.success}")
        print(f"STATISTICS: {result.statistics}")
        for item in result.results:
            print(f"  {item.expectation_config.type}: {'PASS' if item.success else 'FAIL'}")

    passed = sum(outcomes.values())
    print("\n=== SILVER QUALITY SUMMARY ===")
    for table, success in outcomes.items():
        print(f"{table}: {'PASS' if success else 'FAIL'}")
    print(f"TABLES PASSED: {passed}/{len(outcomes)}")

    if passed != len(outcomes):
        raise RuntimeError("Silver quality validation failed")
    print("SILVER_QUALITY_SUCCESS: True")
finally:
    spark.stop()
