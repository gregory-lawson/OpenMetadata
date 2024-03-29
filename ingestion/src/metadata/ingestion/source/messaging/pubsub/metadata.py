"""
PubSub source ingestion
"""
import traceback
from typing import Any, Iterable, List, Optional

from metadata.generated.schema.api.data.createTopic import CreateTopicRequest
from metadata.generated.schema.entity.services.connections.messaging.pubsubConnection import (
    PubsubConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata

from metadata.ingestion.source.messaging.messaging_service import (
    BrokerTopicDetails,
    MessagingServiceSource,
)
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()
MAX_MESSAGE_SIZE = 1_000_000


class PubsubSource(MessagingServiceSource):
    """
    Implements the necessary methods to extract
    topics metadata from PubSub Source

    base.get_topic
    -> (get_topic_list -> get_topic_name)
      -> yield_topic
      -> base.yield_topic_sample_data
    base.mark_topics_as_deleted
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        # self.generate_sample_data = self.config.sourceConfig.config.generateSampleData
        self.pubsub = self.connection

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: PubsubConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, PubsubConnection):
            raise InvalidSourceException(
                f"Expected PubSubConnection, but got {connection}"
            )
        return cls(config, metadata)

    def yield_topic(self, topic_details: Any) -> Iterable[Either[CreateTopicRequest]]:
        """
        Method to Get Messaging Entity
        """
        yield from []



    def get_topic_list(self) -> Optional[List[Any]]:
        """
        Get List of all topics
        """
        yield from []


    def get_topic_name(self, topic_details: Any) -> str:
        """
        Get Topic Name
        """
        yield from []