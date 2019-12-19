from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Transform staging tables into the dim table in RedShift

    :param redshift_con_id: the connection id for RedShift
    :type redshift_con_id: str
    :param table: the target dim table in RedShift
    :type table: str
    :param sql: the sql statement to be executed
    :type sql: str
    :param truncate: whether to clear up the content of the table before inserting
    :type truncate: boolean
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.truncate = truncate
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info("Clearing data from dim table")
            redshift.run(f"DELETE FROM {self.table}")
        redshift.run(f"""
            INSERT INTO {self.table}
            {self.sql}
        """)
        self.log.info('LoadDimensionOperator is implemented')       