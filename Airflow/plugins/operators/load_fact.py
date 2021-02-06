from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''LoadFactOperator:
       This operator allows the user to specify the destination factor table
       to insert data into, and a SQL statement to query the data to insert.
       Args:
       redshift_conn_id: The connection ID to Redshift.
       table: The destination table.
       query: The query to select the data to insert into the table.
    '''
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):

        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("Inserting into {}".format(self.table))
        sql_stmt = 'INSERT INTO {} {};'.format(self.table, self.query)
        redshift_hook.run(sql_stmt)
