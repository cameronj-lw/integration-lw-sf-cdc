
import avro
import grpc
import logging
import os
import time

# native
from domain.events import (
    Event, PortfolioCreatedEvent, PortfolioUpdatedEvent, PortfolioDeletedEvent,
)
from domain.event_handlers import EventHandler
from domain.message_subscribers import MessageSubscriber
from domain.models import Portfolio
from domain.repositories import HeartbeatRepository, ReplayIDRepository
from infrastructure.message_brokers import SalesforcePubSubAPIMessageBroker
from infrastructure.models import SFListenerMode
from infrastructure.util.ChangeEventHeaderUtility import process_bitmap
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import get_log_file_name
from infrastructure.util.PubSub import PubSub



class SalesforcePubSubListener(MessageSubscriber):
    def __init__(self, topic: str, event_handler: EventHandler, 
                    replay_id_repo: ReplayIDRepository, heartbeat_repo: HeartbeatRepository|None=None):
        super().__init__(message_broker=SalesforcePubSubAPIMessageBroker(), topics=[topic], event_handler=event_handler)
        self.topic = topic
        self.config = dict(self.message_broker.config)
        # self.config.update(AppConfig().parser['sf_listener'])

        logging.info(f'Creating {self.cn} with config: {self.config}')
        self.pubsub = PubSub(self.config)
        self.replay_id_repo = replay_id_repo
        self.heartbeat_repo = heartbeat_repo

    def save_heartbeat(self):
        if self.heartbeat_repo:
            # Log file name provides a meaningful name, if app_name is not found
            app_name = os.environ.get('APP_NAME') or get_log_file_name()
            if not app_name:
                # Still not found? Default to class name:
                app_name = self.cn

            # Create heartbeat 
            hb = self.heartbeat_repo.heartbeat_class(group='LW-SF-CDC', name=app_name)

            # If it has a log attribute, populate it with something more meaningful:
            if hasattr(hb, 'log'):
                hb.log = f"HEARTBEAT => {self.cn} consuming {self.topic} messages from Salesforce; using event handler {self.event_handler}"

            # Now we have the heartbeat ready to save. Save it: 
            logging.debug(f'About to save heartbeat to {self.heartbeat_repo.cn}: {hb}')
            res = self.heartbeat_repo.create(hb)

    def save_replay_id(self, replay_id):
        return self.replay_id_repo.create(topic=self.topic, replay_id=replay_id)

    def get_latest_replay_id_str(self):
        last_replay_id = self.replay_id_repo.get(topic=self.topic)
        return str(last_replay_id) if last_replay_id else None  # bytes('', 'utf-8')

    def get_latest_replay_id(self):
        last_replay_id = self.replay_id_repo.get(topic=self.topic)
        return last_replay_id if last_replay_id else bytes('', 'utf-8')

    def deserialize(self, event_msg: bytes) -> Event|None:
        # Get the event payload and schema, then decode the payload
        payload_bytes = event_msg.event.payload
        json_schema = self.pubsub.get_schema_json(event_msg.event.schema_id)
        decoded_event_msg = self.pubsub.decode(json_schema, payload_bytes)
        
        #  A change event contains the ChangeEventHeader field. Check if received event is a change event. 
        if 'ChangeEventHeader' in decoded_event_msg:
            # Decode the bitmap fields contained within the ChangeEventHeader. For example, decode the 'changedFields' field.
            # An example to process bitmap in 'changedFields'
            changed_fields = process_bitmap(avro.schema.parse(json_schema), decoded_event_msg['ChangeEventHeader']['changedFields'])
            change_type = decoded_event_msg['ChangeEventHeader']['changeType']
            field_values = {k: v for k, v in decoded_event_msg.items() if k != 'ChangeEventHeader'}
            changed_field_values = {k: v for k, v in field_values.items() if k in changed_fields}
            
            logging.info(f"Processing the following msg: {decoded_event_msg}")
            logging.info(f"The following fields have changed: {', '.join(changed_fields)}")
            logging.info(f"New values of changed fields: {changed_field_values}")

            # TODO: generate more descriptive event type, based on change_type
            event = PortfolioCreatedEvent(portfolio=Portfolio(**field_values))
            return event

    def process(self, event_msg_batch):
        """ Process the event message(s). 
        This includes deserialization, event handling, and saving replay_id.
        """
        
        # Confusing: The "event_msg_batch" from SF can contain several "event messages" ??? 
        if msg_count := len(event_msg_batch.events):
            logging.info(f'{self.cn} received {msg_count} event messages')
        else:
            # If all requested events are delivered, release the semaphore
            # so that a new FetchRequest gets sent by `PubSub.fetch_req_stream()`.
            if event_msg_batch.pending_num_requested == 0:
                pubsub.release_subscription_semaphore()
        
        # Loop through them:
        success = True  # Could be overridden to False below
        for event_msg in event_msg_batch.events:
            try:
                logging.info(f'{self.cn} processing message:\n{event_msg}')
                deserialized_event = self.deserialize(event_msg)
                handle_result = self.event_handler.handle(deserialized_event)
                if not handle_result:
                    success = False
            except Exception as e:
                logging.exception(f'Exception while processing {event_msg}! {e}')
                success = False
        
        # Finally, save the replay ID if all were successful:
        try:
            if success:
                logging.info(f'Successfully processed the message. Saving replay ID {event_msg_batch.latest_replay_id}')
                self.save_replay_id(event_msg_batch.latest_replay_id)
            else:
                logging.info(f'Message processing failed! Not saving replay ID {event_msg_batch.latest_replay_id}')
                # TODO: Desired failure behaviour? Alert? Retry? Kill this process so it can restart?
        except Exception as e:
            logging.exception(f'Exception while saving replay ID {event_msg_batch.latest_replay_id}! {e}')
            # TODO: further exception behaviour? Retry saving the replay ID?

    def listen(self, mode: SFListenerMode=SFListenerMode.LATEST, last_replay_id: int=None, timeout_sec=None):
        if mode == SFListenerMode.CUSTOM:
            # Fetch the last replay ID if it's needed and not explicitly provided
            if not last_replay_id:
                logging.info(f'Attempting to get replay ID from {self.replay_id_repo.cn}...')
                last_replay_id = self.get_latest_replay_id()
            
            # If there is no pre-existing replay ID saved, switch mode to be EARLIEST:
            if not last_replay_id:
                logging.info(f'Changing mode to EARLIEST after not finding any replay ID')
                mode = SFListenerMode.EARLIEST

        # Listen to SF
        self.pubsub.auth()
        if not timeout_sec:
            timeout_sec = AppConfig().get('sf_pubsub_api', 'timeout_sec', fallback=None)
            timeout_sec = int(timeout_sec) if timeout_sec else None

        logging.info(f'Listening to Salesforce in {mode.name} mode from replay ID {str(last_replay_id)}...')
        while True:
            try:
                # Save heartbeat. The consumer is alive initially.
                self.save_heartbeat()
                last_replay_id = self.get_latest_replay_id()
                self.pubsub.subscribe(self.topic, mode.name, last_replay_id, 1, self.process, 
                    timeout_sec=timeout_sec,
                )
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logging.debug(f"No messages received in {timeout_sec}s — reconnecting...")
                else:
                    logging.error(f"gRPC error: {e}")
                # Sleep to avoid rapid reconnect loops
                time.sleep(1)


