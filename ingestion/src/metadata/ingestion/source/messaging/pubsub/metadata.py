"""
PubSub source ingestion
"""
import traceback
from typing import Iterable, List, Optional

from metadata.generated.schema.api.data.createTopic import CreateTopicRequest
from metadata.generated.schema.entity.services.connections.messaging.pubSubConnection import (
    PubSubConnection,
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
from metadata.ingestion.source.messaging.pubsub.models import (
    PubSubArgs,
    PubSubSummaryModel,
    PubSubTopicMetadataModel,
)
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
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        # self.generate_sample_data = self.config.sourceConfig.config.generateSampleData
        self.pubsub = self.connection

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: PubSubConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, PubSubConnection):
            raise InvalidSourceException(
                f"Expected PubSubConnection, but got {connection}"
            )
        return cls(config, metadata)

    def get_topic_names_list(self) -> List[str]:
        """Get the list of all the topics"""
        all_topics, has_more_topics, args = [], True, PubSubArgs()
        while has_more_topics:
            try:
                topics: Optional[List[object]] = self.pubsub.publisher_client.list_topics(**args.dict())
                for topic in topics:
                    all_topics.append(topic.name)

                has_more_topics = False
            except Exception as err:
                logger.debug(traceback.format_exc())
                logger.error(f"Failed to fetch pubsub topic - {err}")
        return all_topics

    def get_topic_list(self) -> Iterable[BrokerTopicDetails]:
        """Method to yield topic details"""
        all_topics = self.get_topic_names_list()
        for topic_name in all_topics:
            try:
                yield BrokerTopicDetails(
                    topic_name=topic_name,
                    topic_metadata=PubSubTopicMetadataModel(
                        summary=None,  # self._get_topic_details(topic_name),
                        partitions=['a'],  # self._get_topic_partitions(topic_name),
                    ),
                )
            except Exception as err:
                logger.debug(traceback.format_exc())
                logger.error(f"Failed to yield pubsub topic - {err}")

    def yield_topic(
        self, topic_details: BrokerTopicDetails
    ) -> Iterable[Either[CreateTopicRequest]]:
        """Method to yield the create topic request"""
        try:
            logger.info(f"Fetching topic details {topic_details.topic_name}")

            source_url = (
                f"https://pubsub.googleapis.com/pubsub/topics/{topic_details.topic_name}"
            )

            topic = CreateTopicRequest(
                name=topic_details.topic_name,
                service=self.context.messaging_service,
                partitions=len(topic_details.topic_metadata.partitions),
                retentionTime=self._compute_retention_time(
                    topic_details.topic_metadata.summary
                ),
                maximumMessageSize=MAX_MESSAGE_SIZE,
                sourceUrl=source_url,
            )
            yield Either(right=topic)
            self.register_record(topic_request=topic)

        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=topic_details.topic_name,
                    error=f"Unexpected exception to yield topic [{topic_details}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def get_topic_name(self, topic_details: BrokerTopicDetails) -> str:
        return topic_details.topic_name

    def _compute_retention_time(self, summary: Optional[PubSubSummaryModel]) -> float:
        retention_time = 0
        if summary:
            retention_time = (
                summary.TopicDescriptionSummary.RetentionPeriodHours * 3600000
            )
        return float(retention_time)
