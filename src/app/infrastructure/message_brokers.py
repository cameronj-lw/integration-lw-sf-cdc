
# core python

# native
from domain.message_brokers import MessageBroker
from infrastructure.util.config import AppConfig


class SalesforcePubSubAPIMessageBroker(MessageBroker):
    def __init__(self):
        super().__init__(config=AppConfig().parser['sf_pubsub_api'])


