cat > /home/cloudera/healthtrend_trend_job.py <<'EOF'
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

def main(dt):
    sqlContext = SQLContext.getOrCreate(sc)

    inp = "/healthTrend/patient_json/date=%s/*.json" % dt
    out = "/healthTrend/reports/trend_summary/date=%s" % dt

    df = sqlContext.read.json(inp)

    # ensure types
    df = df.withColumn("age", F.col("age").cast("double"))
    df = df.withColumn("gender", F.upper(F.col("gender")))

    # counts by diagnosis and gender
    base = df.groupBy("diagnosis_code").agg(
        F.count("*").alias("count"),
        F.avg("age").alias("avg_age"),
        F.sum(F.when(F.col("gender") == "M", 1).otherwise(0)).alias("male_count"),
        F.sum(F.when(F.col("gender") == "F", 1).otherwise(0)).alias("female_count")
    )

    # gender_ratio = male_count / female_count (avoid divide by zero)
    res = base.withColumn(
        "gender_ratio",
        F.when(F.col("female_count") == 0, F.lit(None)).otherwise(F.col("male_count") / F.col("female_count"))
    ).select(
        "diagnosis_code", "count", "avg_age", "gender_ratio"
    ).orderBy(F.desc("count"))

    # overwrite daily partition output
    # IMPORTANT: Spark creates multiple part files. That's OK.
    res.coalesce(1).write.mode("overwrite").option("header", "true").csv(out)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit healthtrend_trend_job.py YYYY-MM-DD")
        sys.exit(1)
    main(sys.argv[1])
EOF

chmod +x /home/cloudera/healthtrend_trend_job.py
