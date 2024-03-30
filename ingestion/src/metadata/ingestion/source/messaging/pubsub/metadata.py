"""
PubSub source ingestion
"""
import traceback
from typing import Any, Iterable, List, Optional
from datetime import timedelta

from metadata.generated.schema.api.data.createTopic import CreateTopicRequest
from metadata.generated.schema.type.schema import (
    Topic as TopicSchema,
    SchemaType,
    FieldModel,
    DataTypeTopic
)
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
    MessagingServiceSource,
)
from metadata.ingestion.source.messaging.pubsub.connection import (
    PubsubClient
)
from google.pubsub_v1 import Topic, Encoding
from google.pubsub_v1.types.schema import Schema
import avro.schema

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

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata, pipeline_name: Optional[str] = None):
        super().__init__(config, metadata)
        # self.generate_sample_data = self.config.sourceConfig.config.generateSampleData
        self.pubsub: PubsubClient = self.connection
        self.project_id = self.config.serviceConnection.__root__.config.credentials.gcpConfig.projectId.__root__

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: PubsubConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, PubsubConnection):
            raise InvalidSourceException(
                f"Expected PubSubConnection, but got {connection}"
            )
        return cls(config, metadata)

    def yield_topic(self, topic_details: Topic) -> Iterable[Either[CreateTopicRequest]]:
        """
        Method to Get Messaging Entity
        """
        try:
            topic_name = topic_details.name.split('/')[-1]

            topic = CreateTopicRequest(
                name=topic_name,
                displayName=topic_name,
                messageSchema=self._compute_schema(topic_details),
                service=self.context.get().messaging_service,
                retentionTime=self._compute_retention_time(topic_details),
                partitions=1,
            )
            yield Either(right=topic)
            self.register_record(topic_request=topic)

        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=topic_details.name,
                    error=f"Unexpected exception to yield topic [{topic_details}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )


    def get_topic_list(self) -> Optional[List[Topic]]:
        """
        Get List of all topics
        """
        topic_list: List[Topic] = []
        for topic in self.pubsub.publisher_client.list_topics({'project': f'projects/{self.project_id}'}):
            topic_list.append(topic)
        return topic_list


    def get_topic_name(self, topic_details: Topic) -> str:
        """
        Get Topic Name
        """
        return topic_details.name.split('/')[-1]
    
    def _compute_retention_time(self, topic_details: Topic) -> int:
        """
        Return retention duration in milliseconds
        """
        duration: timedelta = topic_details.message_retention_duration
        return duration.total_seconds() * 1000.0
    
    def _compute_schema(self, topic_details: Topic) -> Optional[TopicSchema]:
        """
        Retrieve structured schema information for schematically-defined topics
        """
        if (topic_details.schema_settings):
            result = self.pubsub.schema_client.get_schema({'name': topic_details.schema_settings.schema})
            topic_schema = TopicSchema()
            topic_schema.schemaText = result.definition
            topic_schema.schemaType = self._compute_schema_type(result)
            topic_schema.schemaFields = self._compute_schema_fields(topic_schema.schemaType, result.definition)       
            
            return topic_schema

        return None
    
    def _compute_schema_type(self, schema_record: Schema) -> Optional[SchemaType]:
        """
        Parse the schema type of the PubSub topic
        """
        if (schema_record.type_ == Schema.Type.AVRO):
            return SchemaType.Avro
        elif (schema_record.type_ == Schema.Type.PROTOCOL_BUFFER):
            return SchemaType.Protobuf
        
        return SchemaType.None_
    
    def _compute_schema_fields(self, schema_type: SchemaType, schema_definition: str) -> Optional[List[FieldModel]]:
        """
        Parse the fields of the PubSub schema into structured data
        """
        if (schema_type == SchemaType.Avro):
            fields: List[FieldModel] = []
            schema = avro.schema.parse(schema_definition)
            for field in schema.props['fields']:
                fields.append(FieldModel(
                    name=field.name,
                    dataType=self._compute_avro_type_mapping(field.type.fullname),
                ))

            return fields

        if (schema_type == SchemaType.Protobuf):
            # TODO: Extract field-details from the raw proto spec off the Pubsub topic
            return []
        
        return []
    
    def _compute_avro_type_mapping(self, avro_type: str) -> DataTypeTopic:
        """
        Map from Avro schema types to DataTypeTopic
        """
        type_map = {
            key.lower(): value.value
            for key, value in DataTypeTopic.__members__.items()
        }
        return type_map.get(avro_type.lower(), DataTypeTopic.UNKNOWN.value)
    
    def _compute_proto_type_mapping(self, proto_type: str) -> DataTypeTopic:
        """
        Map from protobuf schema types to DataTypeTopic
        """
        return DataTypeTopic.UNKNOWN
