from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''LoadDimensionOperator:
       This operator allows the user to specify the destination dimension table
       to insert data into, and a SQL statement to query the data to insert.
       Args:
       redshift_conn_id: The connection ID to Redshift.
       clear: Whether to clear the table before inserting into it.
       table: The destination table.
       query: The query to select the data to insert into the table.
    '''
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 clear=False,
                 table='',
                 query='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.clear = clear
        self.table = table
        self.query = query

    def execute(self, context):

        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.clear:
            self.log.info("Clearing {} before inserting.".format(self.table))
            sql_stmt = 'TRUNCATE TABLE {};'.format(self.table)
            redshift_hook.run(sql_stmt)

        self.log.info("Inserting into {}".format(self.table))
        sql_stmt = 'INSERT INTO {} {};'.format(self.table, self.query)
        redshift_hook.run(sql_stmt)
        