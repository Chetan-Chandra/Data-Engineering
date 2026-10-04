# this is a comment  # this is a comment
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

class BaseHook(BaseHook):
    """Base class for all internal hooks to standardize logging and error handling."""
    def __init__(self, conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def get_connection_metadata(self):
        """Standardized connection retrieval with clear error messaging."""
        try:
            return self.get_connection(self.conn_id)
        except Exception:
            self.log.error(f"Failed to find Airflow Connection: {self.conn_id}")
            raise AirflowException(f"Connection {self.conn_id} is missing or inaccessible.")