
# core python
import argparse
import logging
import os
import sys

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from application.event_handlers import PortfolioEventHandler
from infrastructure.models import SFListenerMode
from infrastructure.message_subscribers import SalesforcePubSubListener
from infrastructure.sql_repositories import (
    COREDBReplayIDRepository, 
    MGMTDBHeartbeatRepository,
)
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import setup_logging


def main():
    parser = argparse.ArgumentParser(description='Salesforce listener')
    parser.add_argument(
        '--data_type', '-dt', type=str, required=True
        , help='Type of data to consume'
    )
    parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
    
    args = parser.parse_args()

    if args.data_type == 'portfolio':
        base_dir = AppConfig().get("logging", "base_dir")
        os.environ['APP_NAME'] = AppConfig().get("app_name", "sf_cdc")
        setup_logging(base_dir=base_dir, log_level_override=args.log_level)

        sf_listener = SalesforcePubSubListener(
            topic = AppConfig().get('sf_topics', 'cdc_portfolio'),
            event_handler = PortfolioEventHandler(target_portfolio_repos=[]),  # TODO: add target repos
            replay_id_repo = COREDBReplayIDRepository(),
            heartbeat_repo = MGMTDBHeartbeatRepository(),
        )

        # Start the SF listener
        logging.info(f'Listening to Salesforce for portfolio change events...')
        sf_listener.listen(mode=SFListenerMode.CUSTOM)


if __name__ == '__main__':
    main()
