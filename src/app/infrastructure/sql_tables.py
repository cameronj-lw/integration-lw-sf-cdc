
# core python


# pypi
import sqlalchemy
from sqlalchemy import sql

# native
from infrastructure.util.table import BaseTable, ScenarioTable



""" LWDB """

class LWDBCalendarTable(ScenarioTable):
    config_section = 'lwdb'
    table_name = 'calendar'

    def read_for_date(self, data_date):
        """
        Read all entries for a specific date and with the latest scenario

        :param data_date: The data date
        :return: DataFrame
        """
        if sqlalchemy.__version__ >= '2':
            stmt = sql.select(self.table_def)
        else:
            stmt = sql.select([self.table_def])
        stmt = (
            stmt
            .where(self.c.scenario == self.base_scenario)
            .where(self.c.data_dt == data_date)
        )
        data = self.execute_read(stmt)
        return data


""" MGMTDB """

class MGMTDBMonitorTable(ScenarioTable):
    config_section = 'mgmtdb'
    table_name = 'monitor'

    def read(self, scenario=None, data_date=None, run_group=None, run_name=None, run_type=None, run_host=None, run_status_text=None):
        """
        Read all entries, optionally with criteria

        :return: DataFrame
        """
        stmt = sql.select(self.table_def)
        if scenario is not None:
            stmt = stmt.where(self.c.scenario == scenario)
        if data_date is not None:
            stmt = stmt.where(self.c.data_dt == data_date)
        if run_group is not None:
            stmt = stmt.where(self.c.run_group == run_group)
        if run_name is not None:
            stmt = stmt.where(self.c.run_name == run_name)
        if run_type is not None:
            stmt = stmt.where(self.c.run_type == run_type)
        if run_host is not None:
            stmt = stmt.where(self.c.run_host == run_host)
        if run_status_text is not None:
            stmt = stmt.where(self.c.run_status_text == run_status_text)
        return self.execute_read(stmt)

    def read_for_date(self, data_date):
        """
        Read all entries for a specific date

        :param data_date: The data date
        :returns: DataFrame
        """
        stmt = (
            sql.select(self.table_def)
            .where(self.c.data_dt == data_date)
        )
        data = self.execute_read(stmt)
        return data


""" COREDB """

class COREDBSFReplayIDTable(BaseTable):
    config_section = 'coredb'
    table_name = 'sf_replay_id'

    def read(self, topic: str|None=None, consumer_group: str|None=None):
        """
        Read all entries, optionally with criteria

        :return: DataFrame
        """
        stmt = sql.select(self.table_def)
        if topic is not None:
            stmt = stmt.where(self.c.topic == topic)
        if consumer_group is not None:
            stmt = stmt.where(self.c.consumer_group == consumer_group)
        return self.execute_read(stmt)

    def latest_replay_id(self, topic: str|None=None, consumer_group: str|None=None) -> int:
        """
        Get the highest replay_id, or None if there is none found

        :return: DataFrame
        """
        res_df = self.read(topic=topic, consumer_group=consumer_group)
        if len(res_df):
            return res_df['replay_id'].max()
        else:
            return None


