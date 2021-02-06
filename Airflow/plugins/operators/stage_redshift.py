from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''StageToRedshiftOperator:
       This operator allows the user to specify the source JSON (in S3) and the
       destination table to load the data into.
       Args:
       redshift_conn_id: The connection ID to Redshift.
       aws_credentials_id: The credential ID in Airflow.
       table: The destination table.
       s3_bucket: The name of the S3 bucket.
       s3_key: The name of the S3 key.
       json_args: JSON arguments for the COPY command.
    '''   
    ui_color = '#358140'

    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        COMPUPDATE OFF
        REGION 'us-west-2'
        JSON {}
        TIMEFORMAT 'epochmillisecs';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_args="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_args = json_args

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clear the table before inserting.")
        redshift.run("TRUNCATE TABLE {};".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_args
        )
        redshift.run(formatted_sql)
