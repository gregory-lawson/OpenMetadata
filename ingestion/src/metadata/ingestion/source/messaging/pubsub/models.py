"""
PubSub Models
"""
# Disable pylint to conform to Kinesis API returns
# We want to convert to the pydantic models in 1 go
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Extra


class PubSubTopicModel(BaseModel):
    """
    Model for PubSub topics
    """

    TopicNames: List[str]
    HasMoreTopics: bool


class PubSubArgs(BaseModel):
    """
    Model for PubSub API Arguments
    """

    class Config:
        extra = Extra.allow

    Limit: int = 100

