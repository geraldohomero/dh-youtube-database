from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, List, Dict, Any

@dataclass
class ChannelDetails:
    channel_id: str
    channel_name: str
    subscriber_count: int

@dataclass
class VideoData:
    videoId: str
    channelId: str
    videoTitle: str
    videoAudio: Optional[str]
    videoTranscript: Optional[str]
    transcriptLanguage: Optional[str]
    viewCount: int
    likeCount: int
    commentCount: int
    publishedAt: str
    collectedDate: date
    commentsEnabled: bool

@dataclass
class CommentData:
    commentId: str
    videoId: str
    parentCommentId: Optional[str]
    userId: str
    userName: str
    content: str
    likeCount: int
    publishedAt: str
    collectedDate: date

@dataclass
class ChannelData:
    channelId: str
    channelName: str
    dayCollected: date
    numberOfSubscribers: int
    numberOfVideos: int
