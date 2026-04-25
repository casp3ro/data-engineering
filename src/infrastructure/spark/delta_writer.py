from delta.tables import DeltaTable
from pyspark.sql import DataFrame


class DeltaWriter:
    BASE = "s3a://"

    def write_bronze(self, df: DataFrame, table: str) -> None:
        path = f"{self.BASE}bronze/{table}"
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)

    def upsert_silver(
        self,
        df: DataFrame,
        table: str,
        merge_key: str = "id",
    ) -> None:
        path = f"{self.BASE}silver/{table}"
        spark = df.sparkSession

        if DeltaTable.isDeltaTable(spark, path):
            target = DeltaTable.forPath(spark, path)
            (
                target.alias("target")
                .merge(
                    df.alias("source"),
                    f"target.{merge_key} = source.{merge_key}",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            df.write.format("delta").mode("overwrite").save(path)
