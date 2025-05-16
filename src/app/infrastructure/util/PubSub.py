"""
PubSub.py

SF-provided as part of this quick start: https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-generate-stub.html
With some minor LW tweaks: 
    -use our AppConfig
    -change locations of pb2 stubs
    -log rather than print
    -remove self param in subscribe call to callback
    -connect to SF using REST API oauth (add client ID and secret to facilitate this)
    -support timeout_sec in subscribe to return if no messages are found after this long

This file defines the class `PubSub`, which contains common functionality for
both publisher and subscriber clients.
"""

import io
import threading
import xml.etree.ElementTree as et
from datetime import datetime
from http import HTTPStatus

import avro.io
import avro.schema
import certifi
import grpc
import logging
import requests

from infrastructure.exceptions import SalesforceError
from infrastructure.util import pubsub_api_pb2 as pb2
from infrastructure.util import pubsub_api_pb2_grpc as pb2_grpc
from infrastructure.util.config import AppConfig
from urllib.parse import urlparse
# from utils.ClientUtil import load_properties

# properties = load_properties("../resources/application.properties")
properties = AppConfig().parser['sf_pubsub_api']

with open(certifi.where(), 'rb') as f:
    secure_channel_credentials = grpc.ssl_channel_credentials(f.read())


def get_argument(key, argument_dict):
    if key in argument_dict.keys():
        return argument_dict[key]
    else:
        return properties.get(key)


class PubSub(object):
    """
    Class with helpers to use the Salesforce Pub/Sub API.
    """

    json_schema_dict = {}

    def __init__(self, argument_dict):
        self.url = get_argument('url', argument_dict)
        self.username = get_argument('username', argument_dict)
        self.password = get_argument('password', argument_dict)
        self.client_id = get_argument('client_id', argument_dict)
        self.client_secret = get_argument('client_secret', argument_dict)
        self.tenant_id = get_argument('tenant_id', argument_dict)
        grpc_host = get_argument('grpcHost', argument_dict)
        grpc_port = get_argument('grpcPort', argument_dict)
        pubsub_url = grpc_host + ":" + grpc_port
        channel = grpc.secure_channel(pubsub_url, secure_channel_credentials)
        self.stub = pb2_grpc.PubSubStub(channel)
        self.metadata = None
        self.session = None
        self.session_id = None
        self.pb2 = pb2
        self.topic_name = get_argument('topic', argument_dict)
        # If the API version is not provided as an argument, use a default value
        if get_argument('apiVersion', argument_dict) == None:
            self.apiVersion = '57.0'
        else:
            # Otherwise, get the version from the argument
            self.apiVersion = get_argument('apiVersion', argument_dict)
        """
        Semaphore used for subscriptions. This keeps the subscription stream open
        to receive events and to notify when to send the next FetchRequest.
        See Python Quick Start for more information. 
        https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-python-quick-start.html
        There is probably a better way to do this. This is only sample code. Please
        use your own discretion when writing your production Pub/Sub API client.
        Make sure to use only one semaphore per subscribe call if you are planning
        to share the same instance of PubSub.
        """
        self.semaphore = threading.Semaphore(1)

    def auth(self):
        """
        Copied from libpy39\lib\lw\connector\salesforce.py::SalesforceBase._connect

        Connect to Salesforce. Get token required for REST or Bulk API requests. Connect can be
        called multiple times in a row without issue.

        :return: Response from Salesforce
        """
        logging.debug('Authenticating with Salesforce')
        if self.session is None:
            self.session = requests.Session()

        # Prepare request
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type'   : 'password',
            'username'     : self.username,
            'password'     : self.password,
            'client_id'    : self.client_id,
            'client_secret': self.client_secret,
        }

        # Send request
        result = self.session.post(self.url, headers=headers, data=data)
        if result.status_code != HTTPStatus.OK:
            raise SalesforceError(result)
        logging.debug('Authenticated')
        result = result.json()

        # Extract token, instance, etc.
        self.session_id = result['access_token']
        self.instance_url = result['instance_url']

        # Set metadata headers
        self.metadata = (('accesstoken', self.session_id),
                         ('instanceurl', self.instance_url),
                         ('tenantid', self.tenant_id),
        )

        logging.debug(f'Result from PubSub auth: {result}')
        return result

    def auth_orig(self):
        """
        Sends a login request to the Salesforce SOAP API to retrieve a session
        token. The session token is bundled with other identifying information
        to create a tuple of metadata headers, which are needed for every RPC
        call.
        """
        url_suffix = '/services/Soap/u/' + self.apiVersion + '/'
        headers = {'content-type': 'text/xml', 'SOAPAction': 'Login'}
        xml = "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' " + \
              "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' " + \
              "xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>" + \
              "<urn:login><urn:username><![CDATA[" + self.username + \
              "]]></urn:username><urn:password><![CDATA[" + self.password + \
              "]]></urn:password></urn:login></soapenv:Body></soapenv:Envelope>"
        res = requests.post(self.url + url_suffix, data=xml, headers=headers)
        res_xml = et.fromstring(res.content.decode('utf-8'))[0][0][0]

        try:
            url_parts = urlparse(res_xml[3].text)
            self.url = "{}://{}".format(url_parts.scheme, url_parts.netloc)
            self.session_id = res_xml[4].text
        except IndexError:
            logging.exception("An exception occurred. Check the response XML below:",
            res.__dict__)

        # Get org ID from UserInfo
        uinfo = res_xml[6]
        # Org ID
        self.tenant_id = uinfo[8].text

        # Set metadata headers
        self.metadata = (('accesstoken', self.session_id),
                         ('instanceurl', self.url),
                         ('tenantid', self.tenant_id))

    def release_subscription_semaphore(self):
        """
        Release semaphore so FetchRequest can be sent
        """
        self.semaphore.release()

    def make_fetch_request(self, topic, replay_type, replay_id, num_requested):
        """
        Creates a FetchRequest per the proto file.
        """
        replay_preset = None
        match replay_type:
            case "LATEST":
                replay_preset = pb2.ReplayPreset.LATEST
            case "EARLIEST":
                replay_preset = pb2.ReplayPreset.EARLIEST
            case "CUSTOM":
                replay_preset = pb2.ReplayPreset.CUSTOM
            case _:
                raise ValueError('Invalid Replay Type ' + replay_type)
        return pb2.FetchRequest(
            topic_name=topic,
            replay_preset=replay_preset,
            # replay_id=bytes(replay_id, "utf-8"),
            # replay_id=replay_id.encode("utf-8"),
            replay_id=replay_id,
            num_requested=num_requested)

    def fetch_req_stream(self, topic, replay_type, replay_id, num_requested):
        """
        Returns a FetchRequest stream for the Subscribe RPC.
        """
        while True:
            # Only send FetchRequest when needed. Semaphore release indicates need for new FetchRequest
            self.semaphore.acquire()
            logging.debug("Sending Fetch Request")
            yield self.make_fetch_request(topic, replay_type, replay_id, num_requested)

    def encode(self, schema, payload):
        """
        Uses Avro and the event schema to encode a payload. The `encode()` and
        `decode()` methods are helper functions to serialize and deserialize
        the payloads of events that clients will publish and receive using
        Avro. If you develop an implementation with a language other than
        Python, you will need to find an Avro library in that language that
        helps you encode and decode with Avro. When publishing an event, the
        plaintext payload needs to be Avro-encoded with the event schema for
        the API to accept it. When receiving an event, the Avro-encoded payload
        needs to be Avro-decoded with the event schema for you to read it in
        plaintext.
        """
        schema = avro.schema.parse(schema)
        buf = io.BytesIO()
        encoder = avro.io.BinaryEncoder(buf)
        writer = avro.io.DatumWriter(schema)
        writer.write(payload, encoder)
        return buf.getvalue()

    def decode(self, schema, payload):
        """
        Uses Avro and the event schema to decode a serialized payload. The
        `encode()` and `decode()` methods are helper functions to serialize and
        deserialize the payloads of events that clients will publish and
        receive using Avro. If you develop an implementation with a language
        other than Python, you will need to find an Avro library in that
        language that helps you encode and decode with Avro. When publishing an
        event, the plaintext payload needs to be Avro-encoded with the event
        schema for the API to accept it. When receiving an event, the
        Avro-encoded payload needs to be Avro-decoded with the event schema for
        you to read it in plaintext.
        """
        schema = avro.schema.parse(schema)
        buf = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(buf)
        reader = avro.io.DatumReader(schema)
        ret = reader.read(decoder)
        return ret

    def get_topic(self, topic_name):
        return self.stub.GetTopic(pb2.TopicRequest(topic_name=topic_name),
                                  metadata=self.metadata)

    def get_schema_json(self, schema_id):
        """
        Uses GetSchema RPC to retrieve schema given a schema ID.
        """
        # If the schema is not found in the dictionary, get the schema and store it in the dictionary
        if schema_id not in self.json_schema_dict or self.json_schema_dict[schema_id]==None:
            res = self.stub.GetSchema(pb2.SchemaRequest(schema_id=schema_id), metadata=self.metadata)
            self.json_schema_dict[schema_id] = res.schema_json

        return self.json_schema_dict[schema_id]

    def generate_producer_events(self, schema, schema_id):
        """
        Encodes the data to be sent in the event and creates a ProducerEvent per
        the proto file. Change the below payload to match the schema used.
        """
        payload = {
            "CreatedDate": int(datetime.now().timestamp()),
            "CreatedById": '005R0000000cw06IAA',  # Your user ID
            "textt__c": 'Hello World'
        }
        req = {
            "schema_id": schema_id,
            "payload": self.encode(schema, payload)
        }
        return [req]

    def subscribe(self, topic, replay_type, replay_id, num_requested, callback, timeout_sec=None):
        """
        Calls the Subscribe RPC defined in the proto file and accepts a
        client-defined callback to handle any events that are returned by the
        API. It uses a semaphore to prevent the Python client from closing the
        connection prematurely (this is due to the way Python's GRPC library is
        designed and may not be necessary for other languages--Java, for
        example, does not need this).
        """
        logging.debug(f"> Subscribing to {topic}...")
        while True:
            sub_stream = self.stub.Subscribe(
                self.fetch_req_stream(topic, replay_type, replay_id, num_requested),
                metadata=self.metadata,
                timeout=timeout_sec,
            )
            for event in sub_stream:
                callback(event)

    def publish(self, topic_name, schema, schema_id):
        """
        Publishes events to the specified Platform Event topic.
        """

        return self.stub.Publish(self.pb2.PublishRequest(
            topic_name=topic_name,
            events=self.generate_producer_events(schema,
                                                 schema_id)),
            metadata=self.metadata)