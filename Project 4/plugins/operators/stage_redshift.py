from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credientials_id,
                 redS_conn_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credientials_id = aws_credientials_id
        self.redS_conn_id = redS_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path  = json_path
        


    def execute(self, context):
        """
        Get AWS credential and copy s3 data to redshift staging tables
        """
        aws_hook = AwsHook(self.aws_credientials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redS_conn_id)

        self.s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        self.log.info(f'Staging table {self.table} to Red Shift......Processing')

        redshift.run(f"COPY {self.table} FROM '{s3_path}' \
                       ACCESS_KEY_ID '{credentials.access_key}' \
                       SECRET_ACCESS_KEY '{credentials.secret_key}' \
                       FORMAT AS JSON '{self.json_path}'")

        self.log.info(f'Staging table {self.table} to Red Shift......Completed')
