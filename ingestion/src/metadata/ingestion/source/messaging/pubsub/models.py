"""
PubSub Models
"""
# Disable pylint to conform to PubSub API returns
# We want to convert to the pydantic models in 1 go
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Extra


class PubSubEnum(Enum):
    """
    Enum for PubSub
    """


class PubSubTopicModel(BaseModel):
    """
    Model for PubSub topics
    """

    TopicNames: List[str]
    HasMoreTopics: bool


class PubSubSummaryAttributes(BaseModel):
    """
    Model for PubSub Summary Attributes
    """

    RetentionPeriodHours: Optional[float] = 0


class PubSubSummaryModel(BaseModel):
    """
    Model for PubSub Summary
    """

    TopicDescriptionSummary: PubSubSummaryAttributes


class PubSubTopicMetadataModel(BaseModel):
    """
    Model for PubSub Topic Metadata
    """

    summary: Optional[PubSubSummaryModel]
    partitions: Optional[List[str]]


class PubSubArgs(BaseModel):
    """
    Model for PubSub API Arguments
    """

    class Config:
        extra = Extra.allow

    project: str


class PubSubTopicArgs(BaseModel):
    """
    Model for PubSub Topic API Arguments
    """

    class Config:
        extra = Extra.allow

    TopicName: str


class PubSubShards(BaseModel):
    """
    Model for PubSub Shards
    """

    ShardId: str


class PubSubPartitions(BaseModel):
    """
    Model for PubSub Partitions
    """

    Shards: Optional[List[PubSubShards]]
    NextToken: Optional[str]


class PubSubShardIterator(BaseModel):
    """
    Model for PubSub Shard Iterator
    """

    ShardIterator: Optional[str]


class PubSubData(BaseModel):
    """
    Model for PubSub Sample Data
    """

    Data: Optional[bytes]


class PubSubRecords(BaseModel):
    """
    Model for PubSub Records
    """

    Records: Optional[List[PubSubData]]
