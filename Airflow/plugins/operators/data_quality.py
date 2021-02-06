from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''DataQualityOperator:
       This operator allows the user to specify SQL checks to run on The data
       after it has been imported in the ETL operation.
       Args:
       redshift_conn_id: The connection ID to Redshift.
       dq_checks: A list of dictionary items that specify the SQL to run and the expected output.
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):

        redshift_hook = PostgresHook(self.redshift_conn_id)
        failing_tests = []
        error_count = 0

        for check in self.dq_checks:
        
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
 
            records = redshift_hook.get_records(sql)[0]
 
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
 
        if error_count > 0:
            self.log.info('Data quality check(s) failed:')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        
        self.log.info('Data quality check passed')
