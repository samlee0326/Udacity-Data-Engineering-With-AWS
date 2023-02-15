from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redS_conn_id,
                 table,
                 SQLquery,
                 Truncate_Ops,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redS_conn_id = redS_conn_id
        self.table = table
        self.SQLquery = SQLquery
        self.Truncate_Ops = Truncate_Ops
        
    def execute(self, context):
        redshift_Hook = PostgresHook(self.redS_conn_id)
        if self.Truncate_Ops==True:
            self.log.info(f'Running Truncate statement on table {self.table}')
            redshift_Hook.run(f'TRUNCATE TABLE {self.table_name}')        
        self.log.info(f"Running Query to load data into Fact Table {self.table} ")
        redshift_Hook.run(f"INSERT INTO {self.table} {self.SQLquery}")
        self.log.info(f"Fact Table {self.table}  loaded")
