
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import datetime
from typing import Any, Dict, List, Tuple, Union

# native
from domain.entities import Heartbeat, Portfolio


class PortfolioRepository(ABC):

    @abstractmethod
    def create(self, portfolios: List[Portfolio]|Portfolio) -> int:
        pass

    @abstractmethod
    def get(self, portfolio_code: str|None=None) -> List[Portfolio]:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


class HeartbeatRepository(ABC):
    
    @abstractmethod
    def create(self, heartbeat: Heartbeat) -> int:
        pass

    @abstractmethod
    def get(self, data_date: datetime.date|None=None, group: str|None=None, name: str|None=None) -> List[Heartbeat]:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


class ReplayIDRepository(ABC):

    @abstractmethod
    def create(self, replay_id: int) -> int:
        pass

    @abstractmethod
    def get(self, topic: str, consumer_group: str|None=None) -> int:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


