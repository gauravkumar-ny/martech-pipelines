from martech_pipelines.tasks.base import BaseJob
from martech_pipelines.utils.config import get_db


class UserProfileJob(BaseJob):
    def launch(self):
        self.logger.info("Job is up and running")
        self.spark.sql(f"USE {get_db()}")
        try:
            last_job_run = \
                self.spark.read.table("streaming_job_runs").where(f"query_name = '{self.conf.get('job_name')}'").select(
                    "last_job_run").collect()[0]['last_job_run'].strftime("%Y-%m-%d %H:%M:%S")
        except IndexError as ex:
            last_job_run = None

        self.logger.info(f"Last Job Run time : {last_job_run}")

        if last_job_run is not None:
            changes = self.spark.read.format("delta") \
                .option("readChangeFeed", "true") \
                .option("startingTimestamp", last_job_run) \
                .table("nykaa_user_properties")
        else:
            changes = self.spark.read.format("delta") \
                .option("readChangeFeed", "true") \
                .option("startingVersion", 0) \
                .table("nykaa_user_properties")

        changes.createOrReplaceTempView("changes")
        latest_changes_df = self.spark.sql('''
        with latest_updates_time as (
          select
            customer_id,
            row_number() over (
              partition by customer_id
              order by
                _commit_timestamp desc
            ) as rank,
            _commit_timestamp
          from
            changes
          where
            _change_type in ('insert', 'update_postimage')
        )
        select
          c.*
        from
          changes c
          join latest_updates_time lut on c.customer_id = lut.customer_id
          and c._commit_timestamp = lut._commit_timestamp
        where
          rank = 1
          and _change_type in ('insert', 'update_postimage')''')
        self.logger.info(f"{latest_changes_df.count()}")

        user_property_mapping = self.spark.read.table("nykaa_user_property_mapping")
        attributes = list(user_property_mapping.where(f"{self.conf.get('platform')} = True").select('property_name').collect())
        attributes = [attribute['property_name'] for attribute in attributes]
        self.logger.info(f"{attributes}")

        self.logger.info("Job completed")


if __name__ == "__main__":
    job = UserProfileJob()
    job.launch()
