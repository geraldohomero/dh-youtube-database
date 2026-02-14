CREATE DATABASE IF NOT EXISTS YouTubeStats;
USE YouTubeStats;

CREATE TABLE Channels (
    channelId VARCHAR(255) PRIMARY KEY,
    channelName VARCHAR(255) NOT NULL,
    dayCollected DATE NOT NULL,     
    numberOfSubscribers INT,
    numberOfVideos INT
);

CREATE TABLE Videos (
    videoId VARCHAR(255) PRIMARY KEY,
    channelId VARCHAR(255),
    videoTitle VARCHAR(255) NOT NULL,
    videoAudio TEXT,                     -- Path/URL to audio file
    videoTranscript TEXT,                -- Whisper-generated transcript
    viewCount INT,
    likeCount INT,
    commentCount INT,
    publishedAt DATETIME NOT NULL,      
    collectedDate DATE NOT NULL,         -- Date video data was collected by the researcher
    FOREIGN KEY (channelId) REFERENCES Channels(channelId) ON DELETE CASCADE
);

-- stores both comments and replies
CREATE TABLE Comments (
    commentId VARCHAR(255) PRIMARY KEY,  -- Use YouTube's comment/reply ID
    videoId VARCHAR(255) NOT NULL,       -- Direct link to video
    parentCommentId VARCHAR(255),        -- NULL = top-level comment, NOT NULL = reply
    userId VARCHAR(255) NOT NULL,
    userName VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    likeCount INT,
    publishedAt DATETIME NOT NULL,       -- Original timestamp
    collectedDate DATE NOT NULL,         -- Date data was collected by the researcher
    FOREIGN KEY (videoId) REFERENCES Videos(videoId) ON DELETE CASCADE,
    FOREIGN KEY (parentCommentId) REFERENCES Comments(commentId) ON DELETE CASCADE
);
