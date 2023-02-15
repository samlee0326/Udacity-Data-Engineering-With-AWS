from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redS_conn_id,
                SQLquery,
                table,
                Truncate_Ops = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redS_conn_id = redS_conn_id
        self.SQLquery = SQLquery
        self.table = table
        self.Truncate_Ops = Truncate_Ops

    def execute(self, context):
        redshift_Hook = PostgresHook(self.redS_conn_id)
        if self.Truncate_Ops:
            self.log.info(f'Running Truncate statement on table {self.table}')
            redshift_Hook.run(f'TRUNCATE TABLE {self.table}')
            
        
        self.log.info(f'Running Query to load data into Dimension Table {self.table}....Prossessing')
        redshift_Hook.run(f'INSERT INTO {self.table} {self.SQLquery}')
        self.log.info(f'Running Query to load data into Dimension Table {self.table}....COMPLETE')