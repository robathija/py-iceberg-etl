import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromPubSub
from apache_beam.transforms.window import FixedWindows
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
import json
import logging
import urllib.parse
from datetime import datetime, timezone
import google.auth
from google.auth.transport.requests import Request
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

logging.basicConfig(level=logging.INFO)

def datetime_to_unix_ms(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return int((dt - epoch).total_seconds() * 1000)

def get_access_token(service_account_file, scopes):
    credentials, _ = google.auth.load_credentials_from_file(
        service_account_file, scopes=scopes)
    request = Request()
    credentials.refresh(request)
    return credentials

class ParsePubSubMessage(beam.DoFn):
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        record = json.loads(element.decode('utf-8'))
        record['timestamp'] = timestamp.to_utc_datetime().isoformat()
        yield record

class WriteToIceberg(beam.DoFn):
    def __init__(self, catalog_config, namespace, table_name):
        self.catalog_config = catalog_config
        self.namespace = namespace
        self.table_name = table_name

    def setup(self):
        self.catalog = SqlCatalog("mac_pg_catalog", **self.catalog_config)
        
        # Check if namespace exists, if not create it
        try:
            self.catalog.load_namespace_properties(self.namespace)
            logging.info(f"Namespace {self.namespace} exists")
        except NoSuchNamespaceError:
            self.catalog.create_namespace(self.namespace)
            logging.info(f"Created namespace: {self.namespace}")
        
        # Define Iceberg schema
        self.iceberg_schema = Schema(
            NestedField(1, "store_name", StringType(), required=True),
            NestedField(2, "store_id", StringType(), required=True),
            NestedField(3, "timestamp", TimestamptzType(), required=True),
        )

        # Define PyArrow schema with non-nullable fields
        self.pa_schema = pa.schema([
            ('store_name', pa.string()),
            ('store_id', pa.string()),
            ('timestamp', pa.timestamp('us', tz='UTC'))
        ]).with_metadata({"iceberg.schema": self.iceberg_schema.json()})

        # Make all fields non-nullable
        self.pa_schema = pa.schema([pa.field(f.name, f.type, nullable=False) for f in self.pa_schema])
        
        # Try to load the table, create if it doesn't exist
        try:
            self.table = self.catalog.load_table(f"{self.namespace}.{self.table_name}")
            logging.info(f"Loaded existing table: {self.namespace}.{self.table_name}")
        except NoSuchTableError:
            self.table = self.catalog.create_table(
                identifier=f"{self.namespace}.{self.table_name}", 
                schema=self.iceberg_schema,
                properties={"format-version": "2"}
            )
            logging.info(f"Created new table: {self.namespace}.{self.table_name}")

    def process(self, element):
        records = list(element[1])  # element is now (key, iterable)
        
        store_names = [record['store_name'] for record in records]
        store_ids = [record['store_id'] for record in records]
        timestamps = [datetime.fromisoformat(record['timestamp']) for record in records]

        table_data = pa.Table.from_arrays(
            [
                pa.array(store_names, type=pa.string()),
                pa.array(store_ids, type=pa.string()),
                pa.array(timestamps, type=pa.timestamp('us', tz='UTC'))
            ],
            schema=self.pa_schema
        )

        self.table.append(table_data)
        logging.info(f"Processed {len(records)} records")
        yield element

# Set up pipeline options
options = PipelineOptions([
    '--runner=DirectRunner',  # Change to DataflowRunner for production
    '--project=project-name-dev',
    '--region=US',
    '--temp_location=gs://bucket/temp',
    '--streaming'
])

# Catalog configuration
username = 'pg_user'
password = 'xxxx'
database = 'iceberg-metastore-db'
username_encoded = urllib.parse.quote_plus(username)
password_encoded = urllib.parse.quote_plus(password)

service_account_file = "/Users/username/py-iceberg-etl/creds.json"
scopes = ["https://www.googleapis.com/auth/cloud-platform"]
access_token = get_access_token(service_account_file, scopes)

catalog_config = {
    "uri": f"postgresql+psycopg2://{username_encoded}:{password_encoded}@localhost/{database}",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    "gcs.oauth2.token-expires-at": datetime_to_unix_ms(access_token.expiry),
    "gcs.project-id": "project-name-dev",
    "gcs.oauth2.token": access_token.token,
    "gcs.default-location": "US",
    "warehouse": "gs://bucket/lakehouse"
}

namespace = "mac_namespace"
table_name = "store"

# Pub/Sub topic
topic = "projects/project-name-dev/topics/store-dev-topic"

# Create the pipeline
with beam.Pipeline(options=options) as p:
    (
        p
        | 'Read from Pub/Sub' >> ReadFromPubSub(topic=topic)
        | 'Parse PubSub Messages' >> beam.ParDo(ParsePubSubMessage())
        | 'Window' >> beam.WindowInto(FixedWindows(60))  # 1-minute fixed windows
        | 'Add Key' >> beam.Map(lambda x: (x['timestamp'], x))
        | 'Group' >> beam.GroupByKey()
        | 'Write to Iceberg' >> beam.ParDo(WriteToIceberg(
            catalog_config, namespace, table_name
        ))
    )
