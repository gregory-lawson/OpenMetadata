"""
PubSub source ingestion
"""
import binascii
import traceback
from base64 import b64decode
from typing import Iterable, List, Optional, Any

from metadata.generated.schema.api.data.createTopic import CreateTopicRequest
from metadata.generated.schema.entity.data.topic import TopicSampleData
from metadata.generated.schema.entity.services.connections.messaging.pubSubConnection import (
    PubSubConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.schema import Topic
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.models.ometa_topic_data import OMetaTopicSampleData
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.messaging.pubsub.models import (
    PubSubTopicModel,
    PubSubArgs
)
from metadata.ingestion.source.messaging.messaging_service import (
    BrokerTopicDetails,
    MessagingServiceSource,
)
from metadata.utils import fqn
from metadata.utils.constants import UTF_8
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()
MAX_MESSAGE_SIZE = 1_000_000


class PubsubSource(MessagingServiceSource):
    """
    Implements the necessary methods to extract
    topics metadata from PubSub Source
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        self.generate_sample_data = self.config.sourceConfig.config.generateSampleData
        self.pubSub = self.connection

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: PubSubConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, PubSubConnection):
            raise InvalidSourceException(
                f"Expected PubSubConnection, but got {connection}"
            )
        return cls(config, metadata)

    def get_topic_list(self) -> Optional[List[Any]]:
        return [ 'a topic name', 'another topic name']

    def get_topic_name(self, topic_details: Any) -> str:
        return topic_details

    def yield_topic(self, topic_details: Any) -> Iterable[Either[CreateTopicRequest]]:
        try:
            topic = CreateTopicRequest(
                name=topic_details,
                service=self.context.messaging_service,
                partitions=1
            )
            yield Either(right=topic)
            self.register_record(topic_request=topic)

        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=topic_details,
                    error=f"Unexpected exception to yield topic [{topic_details}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

