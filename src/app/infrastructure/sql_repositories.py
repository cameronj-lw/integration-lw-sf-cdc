
# core python
import datetime
import logging
import os

# native
from domain.models import Heartbeat
from domain.repositories import HeartbeatRepository, ReplayIDRepository
from infrastructure.models import MGMTDBHeartbeat
from infrastructure.sql_tables import COREDBSFReplayIDTable, MGMTDBMonitorTable



""" MGMTDB """

class MGMTDBHeartbeatRepository(HeartbeatRepository):
    # Specify which class callers are recommended to use when instantiating heartbeat instances
    heartbeat_class = MGMTDBHeartbeat
    table = MGMTDBMonitorTable()

    def create(self, heartbeat: Heartbeat) -> int:

        # Create heartbeat instance
        hb_dict = heartbeat.to_dict()

        # Populate with table base scenario
        hb_dict['scenario'] = self.table.base_scenario

        # Truncate asofuser if needed
        hb_dict['asofuser'] = (hb_dict['asofuser'] if len(hb_dict['asofuser']) <= 32 else hb_dict['asofuser'][:32])

        # Columns used as basis for upsert
        pk_columns = ['data_dt', 'scenario', 'run_group', 'run_name', 'run_type', 'run_host', 'run_status_text']

        # Remove columns not in the table def
        hb_dict = {k: hb_dict[k] for k in hb_dict if k in self.table.c.keys()}

        # Bulk insert new rows:
        logging.debug(f"{self.cn}: About to upsert {hb_dict}")
        res = self.table.upsert(pk_column_name=pk_columns, data=hb_dict)  # TODO: error handling?
        if isinstance(res, int):
            row_cnt = res
        else:
            row_cnt = res.rowcount
        if abs(row_cnt) != 1:
            raise Exception(f"Expected 1 row to be saved, but there were {row_cnt}!")
        logging.debug(f'End of {self.cn} create: {heartbeat}')
        return row_cnt

    def get(self, data_date: datetime.date|None=None, group: str|None=None, name: str|None=None) -> list[Heartbeat]:
        # Query table - returns result into df:
        query_result = self.table.read(scenario=self.table.base_scenario, data_date=data_date, run_group=group, run_name=name, run_type='INFO', run_status_text='HEARTBEAT')
        
        # Convert to dict:
        query_result_dicts = query_result.to_dict('records')

        # Create list of heartbeats
        heartbeats = [self.heartbeat_class.from_dict(qrd) for qrd in query_result_dicts]

        # Return result heartbeats list
        return heartbeats

    def __str__(self):
        return str(self.table)

    @classmethod
    def readable_name(self):
        return 'MGMTDB Monitor table'


""" COREDB """

class COREDBReplayIDRepository(ReplayIDRepository):
    table = COREDBSFReplayIDTable()

    def create(self, topic: str, replay_id: int) -> int:
        replay_id_dict = {
            'topic': topic,
            'replay_id': replay_id,
            'modified_by': os.environ.get('APP_NAME'),
            'modified_at': datetime.datetime.now(),
        }
        # insert new replay_id:
        logging.debug(f"{self.cn}: About to insert {replay_id_dict} to {self.table}")
        res = self.table.execute_insert(data=replay_id_dict)  # TODO: error handling?
        if isinstance(res, int):
            row_cnt = res
        else:
            row_cnt = res.rowcount
        if abs(row_cnt) != 1:
            raise Exception(f"Expected 1 row to be saved, but there were {row_cnt}!")
        logging.debug(f'End of {self.cn} create: {replay_id_dict}')
        return row_cnt

    def get(self, topic: str) -> int:
        return self.table.latest_replay_id(topic=topic)

    def __str__(self):
        return str(self.table)



