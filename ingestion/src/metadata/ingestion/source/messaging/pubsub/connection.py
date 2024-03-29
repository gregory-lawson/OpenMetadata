"""
Source connection handler
"""
from dataclasses import dataclass
from typing import Optional

from google.cloud.pubsub import PublisherClient, SchemaServiceClient, SubscriberClient

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.messaging.pubsubConnection import (
    PubsubConnection,
)
from metadata.generated.schema.security.credentials.gcpValues import (
    GcpCredentialsValues,
    MultipleProjectId,
    SingleProjectId,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.credentials import set_google_credentials
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

TIMEOUT_SECONDS = 10


@dataclass
class PubsubClient:
    def __init__(self, publisher_client, schema_client, subscriber_client) -> None:
        self.publisher_client: PublisherClient = publisher_client
        self.schema_client: SchemaServiceClient = schema_client
        self.subscriber_client: SubscriberClient = subscriber_client


def get_connection(
    connection: PubsubConnection
) -> PubsubClient:
    """
    Create connection
    """
    set_google_credentials(gcp_credentials=connection.credentials)
    publisher_client = PublisherClient()
    schema_client = SchemaServiceClient()
    subscriber_client = SubscriberClient()

    return PubsubClient(
        publisher_client=publisher_client,
        schema_client=schema_client,
        subscriber_client=subscriber_client,
    )


def test_connection(
    metadata: OpenMetadata,
    client: PubsubClient,
    service_connection: PubsubConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """

    def get_topics():
        project_path = f'projects/{client.publisher_client.transport._credentials.project_id}'
        return client.publisher_client.list_topics(request={'project': project_path}, timeout=TIMEOUT_SECONDS)


    def get_schemas():
        project_path = f'projects/{client.schema_client.transport._credentials.project_id}'
        return client.schema_client.list_schemas(request={'parent': project_path}, timeout=TIMEOUT_SECONDS)


    def get_subscriptions():
        project_path = f'projects/{client.subscriber_client.transport._credentials.project_id}'
        return client.subscriber_client.list_subscriptions(request={'project': project_path}, timeout=TIMEOUT_SECONDS)


    test_fn = {
        "GetTopics": get_topics,
        "GetSchemas": get_schemas,
        "GetSubscriptions": get_subscriptions,
    }


    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )