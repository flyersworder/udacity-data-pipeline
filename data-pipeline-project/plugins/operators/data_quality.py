from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Perform data quality check for the the tables in RedShift

    :param redshift_con_id: the connection id for RedShift
    :type redshift_con_id: str
    :param table: the target table in RedShift which to perform the quality check
    :type table: str
    :param columns: a list of columns on which the quality check is performed
    :type columns: list
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 columns=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.columns = columns
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        for column in self.columns:
            null_records = redshift_hook.get_records(f"SELECT COUNT({column}) FROM {self.table} WHERE {column} IS NULL")
            if null_records[0][0] > 0:
                raise ValueError(f"Data quality check failed. {column} contained null")
        
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")