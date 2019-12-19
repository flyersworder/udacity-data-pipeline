from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Transform staging tables into the fact table in RedShift

    :param redshift_con_id: the connection id for RedShift
    :type redshift_con_id: str
    :param table: the target fact table in RedShift
    :type table: str
    :param sql: the sql statement to be executed
    :type sql: str
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(f"""
            INSERT INTO {self.table}
            {self.sql}
        """)
        self.log.info('LoadFactOperator is implemented')
