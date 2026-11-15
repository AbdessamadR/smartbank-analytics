import great_expectations as gx
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SmartBankGXTest").getOrCreate()

try:
    df = spark.read.format("delta").load(
        "/opt/spark/data/lakehouse/silver/transactions"
    )

    context = gx.get_context(mode="ephemeral")
    suite = context.suites.add(
        gx.ExpectationSuite(name="transactions_silver_suite")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="tran_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="tran_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="base_amt")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="base_amt", min_value=0
        )
    )

    datasource = context.data_sources.add_spark(name="smartbank_spark")
    asset = datasource.add_dataframe_asset(name="transactions_silver")
    batch_definition = asset.add_batch_definition_whole_dataframe(
        "transactions_batch"
    )

    validation_definition = gx.ValidationDefinition(
        name="validate_transactions_silver",
        data=batch_definition,
        suite=suite,
    )
    validation_definition = context.validation_definitions.add(
        validation_definition
    )

    result = validation_definition.run(
        batch_parameters={"dataframe": df}
    )

    print("GX_SUCCESS:", result.success)
    print("GX_STATISTICS:", result.statistics)

    for item in result.results:
        print(item.expectation_config.type, "->", item.success)

    if not result.success: 
        raise RuntimeError("Silver transactions quality validation failed")

finally:
    spark.stop()
