
from json.decoder import JSONDecodeError

class SalesforceError(Exception):
    """Copied from libpy39\lib\lw\connector\salesforce.py"""
    """Base Salesforce API exception"""
    message = 'Status:{status}  Message:{content}'

    def __init__(self, result):
        """
        Create a SalesforceError from an API response

        :param result: The result returned from requests.[post|get]
        """
        try:
            content = result.json()
        except JSONDecodeError:
            content = result.text

        self.status = result.status_code
        self.content = content
        self.url = result.url
        super().__init__()

    def __str__(self):
        return self.message.format(status=self.status, content=self.content)

