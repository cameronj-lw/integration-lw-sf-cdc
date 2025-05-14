
import logging

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

    def save_replay_id(self, replay_id):
        return self.replay_id_repo.create(topic=self.topic, replay_id=replay_id)

    def deserialize(self, event_msg: bytes) -> Event|None:
        # Get the event payload and schema, then decode the payload
        payload_bytes = event_msg.event.payload
        json_schema = self.pubsub.get_schema_json(event_msg.event.schema_id)
        decoded_event_msg = self.pubsub.decode(json_schema, payload_bytes)
        
        #  A change event contains the ChangeEventHeader field. Check if received event is a change event. 
        if 'ChangeEventHeader' in decoded_event_msg:
            # Decode the bitmap fields contained within the ChangeEventHeader. For example, decode the 'changedFields' field.
            # An example to process bitmap in 'changedFields'
            changed_fields = decoded_event_msg['ChangeEventHeader']['changedFields']
            change_type = decoded_event_msg['ChangeEventHeader']['changeType']
            field_values = {k: v for k, v in decoded_event_msg.items() if k != 'ChangeEventHeader'}

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

    def listen(self, mode: SFListenerMode=SFListenerMode.LATEST, last_replay_id: int=None):
        if mode == SFListenerMode.CUSTOM:
            # Fetch the last replay ID if it's needed and not explicitly provided
            if not last_replay_id:
                logging.info(f'Attempting to get replay ID from {self.replay_id_repo.cn}...')
                last_replay_id = self.replay_id_repo.get(topic=self.topic)
            
            # If there is no pre-existing replay ID saved, switch mode to be EARLIEST:
            if not last_replay_id:
                logging.info(f'Changing mode to EARLIEST after not finding any replay ID')
                mode = SFListenerMode.EARLIEST

        # Listen to SF
        self.pubsub.auth()
        last_replay_id_str = last_replay_id if last_replay_id else bytes('', 'utf-8')
        logging.info(f'Listetning to Salesforce in {mode.name} mode from replay ID {last_replay_id}...')
        self.pubsub.subscribe(self.topic, mode.name, last_replay_id_str, 1, self.process)


