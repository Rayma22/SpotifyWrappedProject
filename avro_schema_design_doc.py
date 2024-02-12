# AVRO Schema Design Document

from fastavro import parse_schema

schema_ind = {
  "type": "record",
  "name": "SpotifyWrappedData",
  "namespace": "com.spotify.wrapped",
  "fields": [
    {
      "name": "UserProfile",
      "type": {
        "type": "record",
        "name": "UserProfileRecord",
        "fields": [
          {"name": "UserId", "type": "int"},
          {"name": "Age", "type": "string"},
          {"name": "Gender", "type": "string"},
          {"name": "ListeningDevice", "type": "string"},
          {"name": "SubscriptionPlan", "type": "string"},
          {"name": "MusicTimeSlot", "type": "string"},
          {"name": "Location",  "type": "string"},
      ]
      }
    },
    {
      "name": "SongMetadata",
      "type": {
        "type": "record",
        "name": "SongMetadataRecord",
        "fields": [
          {"name": "SongId", "type": "string"},
          {"name": "Title", "type": "string"},
          {"name": "Artist", "type": "string"},
          {"name": "TopGenre", "type": "string"},
          {"name": "Year", "type": "int"},
          {"name": "Length", "type": "string"},
        ]
      }
    },
    {
      "name": "UserSongInteraction",
      "type": {
        "type": "record",
        "name": "UserSongInteractionRecord",
        "fields": [
          {"name": "UserId", "type": "int"},
          {"name": "SongId", "type": "string"},
          {"name": "InteractionType","type": {"type": "enum","name": "InteractionTypeEnum",
              "symbols": ["PLAY", "PAUSE", "SKIP", "LIKE", "DISLIKE"]},},
          {"name": "InteractionTimestamp", "type": "string"},
        ]
      }
    }
  ]
}

parsed_schema_ind = parse_schema(schema_ind)

print(parsed_schema_ind)
