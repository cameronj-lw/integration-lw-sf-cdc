
# core python
from abc import ABC
from dataclasses import dataclass

# native
from domain.models import Portfolio


class Event(ABC):
    """ Base class for domain events """
    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


@dataclass
class PortfolioCreatedEvent(Event):
    portfolio: Portfolio


@dataclass
class PortfolioUpdatedEvent(Event):
    portfolio_before: Portfolio
    portfolio_after: Portfolio


@dataclass
class PortfolioDeletedEvent(Event):
    portfolio: Portfolio
