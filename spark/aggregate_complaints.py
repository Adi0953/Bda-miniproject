"""Aggregate railway complaint records with Apache Spark."""
import sys

from pyspark.sql import SparkSession, functions as F


def main(input_path: str, output_dir: str) -> None:
    spark = SparkSession.builder.appName("RailBigDataComplaintAnalytics").getOrCreate()
    source = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )
    columns = {name.lower(): name for name in source.columns}
    description_col = columns.get("description") or columns.get("sentimenttext")
    if not description_col:
        raise ValueError("CSV needs a description or SentimentText column")

    date_col = columns.get("date")
    type_col = columns.get("complaint_type")
    zone_col = columns.get("zone")
    impact_col = columns.get("impact_score")
    label_col = columns.get("priority") or columns.get("sentiment")
    complaints = source.select(
        (F.to_date(F.col(date_col)).alias("date") if date_col else F.lit(None).cast("date").alias("date")),
        (F.col(type_col).alias("complaint_type") if type_col else F.lit("Other").alias("complaint_type")),
        (F.col(zone_col).alias("zone") if zone_col else F.lit("Unknown").alias("zone")),
        (F.col(label_col).cast("string").alias("source_label") if label_col else F.lit(None).cast("string").alias("source_label")),
        F.col(description_col).cast("string").alias("description"),
        (F.col(impact_col).cast("int").alias("impact_score") if impact_col else F.lit(0).alias("impact_score")),
    )

    description = F.lower(F.coalesce(F.col("description"), F.lit("")))
    impact = F.coalesce(F.col("impact_score").cast("int"), F.lit(0))
    if not type_col:
        inferred_type = (
            F.when(description.rlike("fire|smoke|track|signal|accident|unsafe|electrical"), "Track safety")
            .when(description.rlike("harass|assault|theft|security|unattended bag"), "Security")
            .when(description.rlike("delay|late|cancel|departure"), "Delay")
            .when(description.rlike("dirty|clean|toilet|washroom|garbage|trash|mess"), "Cleanliness")
            .when(description.rlike("ticket|pnr|refund|booking|irctc|fare"), "Ticketing")
            .when(description.rlike("wheelchair|accessib|berth|seat|coach|staff|conductor|official|help"), "Staff / coach assistance")
            .when(description.rlike("water|food|air condition|fan|light|lift|facility|amenit"), "Facilities")
            .otherwise("Other")
        )
        complaints = complaints.withColumn("complaint_type", inferred_type)
    safety = description.rlike(
        "fire|smoke|injur|accident|harass|assault|theft|unsafe|emergency|track|signal failure|electrical|unattended bag"
    )
    priority = (
        F.when(safety | (impact >= 5), "Critical")
        .when(
            (impact >= 4)
            | description.rlike("wheelchair|accessibility|cancelled|stranded|lift|elevator|ramp"),
            "High",
        )
        .when(
            (impact >= 3) | description.rlike("delay|staff conduct|air conditioning"),
            "Medium",
        )
        .otherwise("Low")
    )
    enriched = complaints.withColumn("month", F.date_format(F.to_date("date"), "yyyy-MM"))
    enriched = enriched.withColumn("predicted_priority", priority)

    aggregations = {
        "by_month": enriched.filter(F.col("month").isNotNull()).groupBy("month").count(),
        "by_category": enriched.groupBy("complaint_type").count(),
        "by_zone": enriched.groupBy("zone").count(),
        "by_priority": enriched.groupBy("predicted_priority").count(),
        "by_source_label": enriched.filter(F.col("source_label").isNotNull()).groupBy("source_label").count(),
    }
    for name, frame in aggregations.items():
        frame.write.mode("overwrite").option("header", True).csv(f"{output_dir}/{name}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: spark-submit aggregate_complaints.py <input.csv> <output_dir>")
    main(sys.argv[1], sys.argv[2])
