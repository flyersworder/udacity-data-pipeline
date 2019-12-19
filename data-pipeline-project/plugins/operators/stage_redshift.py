from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Load data from S3 to the staging tables in RedShift

    :param redshift_con_id: the connection id for RedShift
    :type redshift_con_id: str
    :param aws_credentials_id: reference to AWS credentials
    :type aws_credentials_id: str
    :param table: the staging table in RedShift to copy S3 data into
    :type table: str
    :param s3_bucket: name of s3 bucket
    :type s3_bucket: str
    :param s3_key: parameterized s3 folders
    :type s3_key: str
    :param file_format: format of the data files in s3
    :type file_format: str
    :param manifest_file: path of the manifest file that defines copying rules
    :type manifest_file: str
    :param extra_params: (optional) extra parameters provided for the copying command
    :type extra_params: str
    """
    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS {} '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="JSON",
                 manifest_file="",
                 extra_params="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.manifest_file = manifest_file
        self.extra_params = extra_params
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from staging Redshift table")
        redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info("Copying data from S3 to staging Redshift table")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.file_format,
            self.manifest_file,
            self.extra_params
        )
        redshift.run(formatted_sql)
        self.log.info('StageToRedshiftOperator is implemented')





