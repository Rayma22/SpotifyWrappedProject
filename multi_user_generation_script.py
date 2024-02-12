# Multi User Generation Script

from fastavro.schema import load_schema
import fastavro
from fastavro import parse_schema
import pandas as pd
import uuid
from datetime import datetime, timedelta, timezone
import random
from faker import Faker
from timezonefinder import TimezoneFinder
import pytz
import numpy as np
from fastavro import writer as avro_writer

import os

print(os.getcwd())

os.chdir(os.path.dirname(os.path.realpath(__file__)))

print(os.getcwd())

music_data = pd.read_csv('Spotify-2000.csv')
user_data = pd.read_csv('Spotify_data.csv')

fake = Faker()

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

parsed_schema = fastavro.parse_schema(schema_ind)

gender_counts = user_data['Gender'].value_counts(normalize=True)
gender_options = gender_counts.index.tolist()
gender_probabilities = gender_counts.values.tolist()
age_counts = user_data['Age'].value_counts(normalize=True)
age_options = age_counts.index.tolist()
age_probabilities = age_counts.values.tolist()
subscription_plan_counts = user_data['spotify_subscription_plan'].value_counts(normalize=True)
subscription_plan_options = subscription_plan_counts.index.tolist()
subscription_plan_probabilities = subscription_plan_counts.values.tolist()


def generate_listening_session(user_id, start_time, end_time, preferred_genre):
    records = []
    session_start = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S%z')
    session_end = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S%z')

    location = f"{fake.city()}, {fake.country()}"

    #time_zone_str = TimezoneFinder().timezone_at(lat=latitude, lng=longitude)
    #localTimeZone = pytz.timezone(time_zone_str)
    #format = '%Y:%m:%dT%H:%M:%S%z'


    while session_start < session_end:
        # Filter songs by preferred genre
        filtered_songs = music_data[music_data['Top Genre'] == preferred_genre].to_dict('records')
        # Check if filtered_songs is not empty
        if filtered_songs:
            song = random.choice(filtered_songs)
        else:
            # Handle the case where no songs match the preferred genre
            print(f"No songs found for genre {preferred_genre}, selecting a random song instead.")
            song = random.choice(music_data.to_dict('records'))

        interaction_type = random.choice(["PLAY", "PAUSE", "SKIP", "LIKE", "DISLIKE"])

        interaction_timestamp = session_start.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S%z')

        record = {
            "UserProfile": {
                "UserId": user_id,
                "Age": np.random.choice(age_options, p=age_probabilities),
                "Gender": np.random.choice(gender_options, p=gender_probabilities),
                "ListeningDevice": random.choice(["smartphone", "computer", "speaker", "voice assistant", "wearable device"]),
                "SubscriptionPlan": np.random.choice(subscription_plan_options, p=subscription_plan_probabilities),
                "MusicTimeSlot": session_start.strftime('%p'),
                "Location": location
            },
            "SongMetadata": {
                "SongId": str(uuid.uuid4()),
                "Title": song['Title'],
                "Artist": song['Artist'],
                "TopGenre": preferred_genre,
                "Year": song['Year'],
                "Length": song['Length (Duration)']
            },
            "UserSongInteraction": {
                "UserId": user_id,
                "SongId": str(uuid.uuid4()),
                "InteractionType": interaction_type,
                "InteractionTimestamp": interaction_timestamp
            }
        }
        records.append(record)

        # Simulate continuous play
        session_start += timedelta(minutes=4) # timedelta(seconds=song['Length (Duration)']).astimezone(localTimeZone).strftime(format)  # Simplified assumption

    return records


def generate_multiple_users_sessions(num_users, start_time, end_time, genre_list):
    all_records = []

    for i in range(num_users):
        user_id = str(uuid.uuid4())
        preferred_genre = random.choice(genre_list)  # Randomly select a preferred genre for each user

        # Generate a listening session for each user
        user_records = generate_listening_session(i, start_time, end_time, preferred_genre)
        all_records.extend(user_records)  # Add the user's records to the overall list

    return all_records

num_users = 10  # Number of users to generate sessions for
start_time = '2021-04-01T10:00:00-00:00'
end_time = '2021-04-01T12:00:00-00:00'
genre_list = ["dance pop", "modern rock", "detroit hip hop", "electro"]  # Example genre list

# Generating sessions for multiple users
multiple_users_records = generate_multiple_users_sessions(num_users, start_time, end_time, genre_list)
print(multiple_users_records)

# Serialization to AVRO file
output_file = 'multiple_users_spotify_data.avro'
with open(output_file, 'wb') as out:
    avro_writer(out, parsed_schema, multiple_users_records)

print(f"Generated {len(multiple_users_records)} records for {num_users} users and serialized to {output_file}.")

input('Press <ENTER> to exit:')
