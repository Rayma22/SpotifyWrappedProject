#Overview
This project aims to simulate the Spotify Wrapped experience by generating synthetic data that mimics user interactions with a music streaming service. We developed a codebase that creates an AVRO schema for song play data and scripts to generate realistic, time-series data for both single and multiple users' streaming experiences. This document explains the code structure, the rationale behind our approach, and how it aligns with the project objectives.

#Code Explanation
Library Import
The code begins with importing necessary libraries:

fastavro for AVRO schema parsing and serialization.
pandas for data manipulation and analysis.
uuid for generating unique identifiers.
datetime, timedelta, timezone for handling date and time information.
random and faker for generating random data points and fake data that resembles real-world data.
timezonefinder and pytz for determining time zones based on geographical coordinates.
The choice of these libraries was driven by the need to handle complex data structures, manipulate dates and times accurately, and generate realistic synthetic data.

#Data Preparation 
A CSV file named spotify_data is loaded using pandas. This file is user-centric and contains characteristics of the user-behavior such as listening preferences, gender, age, subscription plan etc. Along with this, we imported a dataset called Spotify-2000 which contains additional Spotify data in order to more accurately simulate the data and reflect realistic song choices, including song IDs, names, and artists.

#Pre-processing 
Not all spotify users are made alike, individual users have different behavior trends within the app that can highly depend on age, gender, and subscription. These main categories are imperative in determining how many skips, replays, and type of music that is desired. Therefore, when simulating behavior, it is not completely random, but a collection of several factors. That is why we created probabilities for each of the three categories reflecting their presence within the dataset in order to simulate the most accurate representation of users. 

#AVRO Schema Definition
An AVRO schema named SpotifyWrappedData is defined in the schema_ind dictionary. The schema is divided into 3 main records; UserProfile, SongMetadata, UserSongInteraction which includes various fields that are essential for capturing detailed information about each song play and user behaviour.  The schema is built up of the following structures: 
name - name of the data structure 
type - type of schema defining complexity 
that contain several varying fields in order to define each record.
Examples include but are not limited to; SongId, Artists, UserId, Length etc. 

This schema is designed to encapsulate all relevant data points for simulating Spotify Wrapped data, adhering to the initial instructions to create a comprehensive AVRO schema for what realistic behavior will look like. 

#Single User Simulation 
The function generate_listening sessions is defined taking Userid, start_time, end_time, and preffered_genre as its input variables. The simulations rely on UTC timing to maintain the timestamp. We then parse the schema using fastavro to convert strings to datetime objects. A while loop is defined in order to iterate through the session until time is reached. Songs are then filtered through the dataset based on the inputted preffered genre, and a random song is filtered from this list.A random interaction type (play, pause, skip) is selected for the user's interaction with the song and stored in a record.
Each record contains information about the user's profile, the song being played, and the interaction between the user and the song.
The simulation increments the session start time by a fixed duration (4 minutes in this case) for each iteration of the loop to simulate continuous play.The function finally  returns a list of records representing the simulated listening session.


#Multiple Users Simulation
Similarly, we simulate the behavior of several users in a function called generate_multiple_users_session, calling for the number of users start and end time, and a list of genres that the user will randomly pick from. The function iteraties over the chosen number of users and for each user, a listening session is generated using the generate_listening_session function. The start and end times and the preferred genre are passed as parameters to simulate the session. The records generated for each user are aggregated into a single list containing records for all users, and he function returns the aggregated list of records representing sessions for all users.

#Rationale Behind the Approach
The development of this code was guided by the project's objective to realistically simulate user interactions with a music streaming service over time. It was important for us to consider the main factors behind user behavior such as age, gender, and spotify subscription given the fact that free spotify users have limited skips and cannot choose their songs freely. For the future we can specify more historical behavior in order to more accurately simulate data for each user based on their demographics.

#Conclusions and Challenges
Some of the challenges faced include maintaining consistency in data types and structures across records, ensuring that all timestamps in the generated data are timezone-aware and simulating realistic user behavior patterns, such as the tendency to listen to certain genres or artists more frequently or to use specific devices for streaming. Overall, striking the right balance between variety in the generated data and adhering to realistic distributions of attributes (e.g., gender, subscription plan) was challenging, but we managed produce desirred outcome. 

By creating a detailed AVRO schema and generating synthetic data that closely mimics real-world behaviors, we aim to provide a robust dataset for analyzing user engagement, preferences, and streaming patterns.


