
# core python
from dataclasses import dataclass
import random

# native
from domain.event_handlers import EventHandler
from domain.events import (Event, PortfolioCreatedEvent, PortfolioUpdatedEvent, PortfolioDeletedEvent)
from domain.models import Portfolio
from domain.repositories import PortfolioRepository


@dataclass
class PortfolioEventHandler(EventHandler):
    target_portfolio_repos: list[PortfolioRepository]  # Where to save the latest version of this portfolio

    def handle(self, event: PortfolioCreatedEvent|PortfolioUpdatedEvent|PortfolioDeletedEvent) -> int|None:
        # TODO: Actually do something here. Return False for fail or True for success.

        # Randomly succeed or fail (coinflip)
        return False if random.random() < 0.5 else True


