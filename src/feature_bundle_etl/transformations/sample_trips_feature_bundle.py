
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ==================================================
# BRONZE: BUSINESS-PROVIDED DATA
# ==================================================

@dp.materialized_view(name="bronze_customers")
def bronze_customers():
    data = [
        ("C101", "John", "2024-01-01"),
        ("C102", "Sarah", "2026-09-01"),
        ("C103", "David", "2025-06-01"),
        ("C104", "Emma", "2026-01-01")
    ]

    return spark.createDataFrame(
        data,
        ["customer_id", "customer_name", "account_open_date"]
    )


@dp.materialized_view(name="bronze_transactions")
def bronze_transactions():
    data = [
        ("T001", "C101", 100.0, "2026-11-01 09:00:00"),
        ("T002", "C101", 200.0, "2026-11-03 10:00:00"),
        ("T003", "C101", 300.0, "2026-11-05 11:00:00"),
        ("T004", "C101", 5000.0, "2026-11-07 12:00:00"),
        ("T005", "C102", 50.0, "2026-11-02 09:00:00"),
        ("T006", "C102", 100.0, "2026-11-04 10:00:00"),
        ("T007", "C102", 1200.0, "2026-11-06 11:00:00"),
        ("T008", "C103", 80.0, "2026-11-03 09:00:00"),
        ("T009", "C103", 90.0, "2026-11-05 10:00:00"),
        ("T010", "C103", 95.0, "2026-11-08 11:00:00"),
        ("T011", "C104", 500.0, "2026-11-04 09:00:00"),
        ("T012", "C104", 3000.0, "2026-11-09 10:00:00")
    ]

    return spark.createDataFrame(
        data,
        ["transaction_id", "customer_id",
         "amount", "transaction_time"]
    )


@dp.materialized_view(name="bronze_fraud_labels")
def bronze_fraud_labels():
    data = [
        ("T001", 0),
        ("T002", 0),
        ("T003", 0),
        ("T004", 1),
        ("T005", 0),
        ("T006", 0),
        ("T007", 1),
        ("T008", 0),
        ("T009", 0),
        ("T010", 0),
        ("T011", 0),
        ("T012", 1)
    ]

    return spark.createDataFrame(
        data,
        ["transaction_id", "is_fraud"]
    )


# ==================================================
# SILVER: CLEAN AND VALIDATE
# ==================================================

@dp.materialized_view(name="silver_customers")
@dp.expect_or_drop(
    "valid_customer",
    "customer_id IS NOT NULL AND account_open_date IS NOT NULL"
)
def silver_customers():
    return (
        spark.read.table("bronze_customers")
        .withColumn(
            "account_open_date",
            F.to_date("account_open_date")
        )
        .dropDuplicates(["customer_id"])
    )


@dp.materialized_view(name="silver_transactions")
@dp.expect_or_drop(
    "valid_transaction",
    "transaction_id IS NOT NULL "
    "AND customer_id IS NOT NULL "
    "AND amount > 0 "
    "AND transaction_time IS NOT NULL"
)
def silver_transactions():
    return (
        spark.read.table("bronze_transactions")
        .withColumn(
            "transaction_time",
            F.to_timestamp("transaction_time")
        )
        .dropDuplicates(["transaction_id"])
    )


@dp.materialized_view(name="silver_fraud_labels")
@dp.expect_or_drop(
    "valid_label",
    "transaction_id IS NOT NULL AND is_fraud IN (0, 1)"
)
def silver_fraud_labels():
    return (
        spark.read.table("bronze_fraud_labels")
        .dropDuplicates(["transaction_id"])
    )


# ==================================================
# GOLD 1: CUSTOMER FEATURES AS OF A FIXED DATE
# ==================================================

@dp.materialized_view(name="gold_customer_features")
def gold_customer_features():

    # Fixed reference time makes this demo reproducible.
    as_of = F.to_timestamp(F.lit("2026-11-10 00:00:00"))

    tx = (
        spark.read.table("silver_transactions")
        .filter(F.col("transaction_time") < as_of)
        .filter(
            F.col("transaction_time") >=
            as_of - F.expr("INTERVAL 7 DAYS")
        )
    )

    aggregates = (
        tx.groupBy("customer_id")
        .agg(
            F.count("*").alias("transaction_count_7d"),
            F.avg("amount").alias("avg_amount_7d"),
            F.max("amount").alias("max_amount_7d")
        )
    )

    customers = spark.read.table("silver_customers")

    return (
        customers
        .join(aggregates, "customer_id", "left")
        .select(
            "customer_id",
            F.coalesce(
                F.col("transaction_count_7d"),
                F.lit(0)
            ).alias("transaction_count_7d"),
            F.coalesce(
                F.col("avg_amount_7d"),
                F.lit(0.0)
            ).alias("avg_amount_7d"),
            F.coalesce(
                F.col("max_amount_7d"),
                F.lit(0.0)
            ).alias("max_amount_7d"),
            F.datediff(
                F.to_date(as_of),
                F.col("account_open_date")
            ).alias("account_age_days")
        )
    )


# ==================================================
# GOLD 2: LABELED TRANSACTIONS FOR TRAINING
# ==================================================

@dp.materialized_view(name="gold_training_observations")
def gold_training_observations():

    transactions = spark.read.table("silver_transactions")
    labels = spark.read.table("silver_fraud_labels")

    return (
        transactions
        .join(labels, "transaction_id", "inner")
        .select(
            "transaction_id",
            "customer_id",
            "transaction_time",
            "amount",
            "is_fraud"
        )
    )


# ==================================================
# GOLD 3: HISTORICAL FEATURES FOR TRAINING
# ==================================================

@dp.materialized_view(name="gold_historical_features")
def gold_historical_features():

    tx = spark.read.table("silver_transactions")
    customers = spark.read.table("silver_customers")

    current_tx = tx.alias("current")
    history_tx = tx.alias("history")

    # For each transaction, use only earlier transactions.
    # transaction_id provides a deterministic tie-breaker
    # for records sharing an event timestamp.
    joined = current_tx.join(
        history_tx,
        (
            (F.col("current.customer_id") ==
             F.col("history.customer_id"))
            &
            (F.col("history.transaction_time") >=
             F.col("current.transaction_time") -
             F.expr("INTERVAL 7 DAYS"))
            &
            (F.col("history.transaction_time") <
             F.col("current.transaction_time"))
        ),
        "left"
    )

    features = (
        joined.groupBy(
            F.col("current.transaction_id").alias("transaction_id"),
            F.col("current.customer_id").alias("customer_id"),
            F.col("current.transaction_time").alias("feature_timestamp")
        )
        .agg(
            F.count("history.transaction_id").alias(
                "transaction_count_7d"
            ),
            F.coalesce(
                F.avg("history.amount"), F.lit(0.0)
            ).alias("avg_amount_7d"),
            F.coalesce(
                F.max("history.amount"), F.lit(0.0)
            ).alias("max_amount_7d")
        )
    )

    return (
        features
        .join(
            customers.select("customer_id", "account_open_date"),
            "customer_id",
            "left"
        )
        .withColumn(
            "account_age_days",
            F.datediff(
                F.to_date("feature_timestamp"),
                F.col("account_open_date")
            )
        )
        .drop("account_open_date")
    )
