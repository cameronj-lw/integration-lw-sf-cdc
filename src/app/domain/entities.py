
# core python
from dataclasses import dataclass, field
import datetime
from types import SimpleNamespace



class Portfolio(SimpleNamespace):
    """ Facilitates object instance creation from dict """
    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        try:
            return f"{self.LW_Portfolio_Id__c}: {self.Name}"
        except Exception as e:
            return super().__str__()


@dataclass
class Heartbeat:
    group: str
    name: str
    data_date: datetime.date = field(default_factory=datetime.date.today)
    modified_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def to_dict(self):
        """ Export an instance to dict format """
        return {
            'group': self.group
            , 'name': self.name
            , 'data_date': self.data_date.isoformat() if self.data_date else self.data_date
            , 'modified_at': self.modified_at.isoformat() if self.modified_at else self.modified_at
        }

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        try:
            # Validate required fields
            group = data['group']
            name = data['name']

            # Optional fields
            data_date = data.get('data_date', datetime.date.today())
            if isinstance(data_date, str):
                data_date = datetime.date.fromisoformat(data_date)

            modified_at = data.get('modified_at', datetime.datetime.now())
            if isinstance(modified_at, str):
                modified_at = datetime.datetime.fromisoformat(modified_at)

            return cls(group=group, name=name, data_date=data_date, modified_at=modified_at)
        except KeyError as e:
            raise InvalidDictError(f"Missing required field: {e}")

