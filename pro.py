'''
lSP-projectink of github:https://github.com/zeidg/project.git
'''
import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize, StandardScaler
from multiprocessing import Pool, cpu_count
import requests

# Function to process a chunk of the DataFrame
def process_chunk(chunk):
    # Replace values in 'duration'
    chunk["duration"].replace(
        {"1 Seasons": "120 min", "2 Seasons": "240 min", "3 Seasons": "360 min"},
        inplace=True,
    )
    chunk["duration"] = chunk["duration"].str.replace(" min", "").astype(np.int16)

    # Replace values in 'rating_for_ages'
    chunk["rating_for_ages"].replace(
        {
            "G": "0 +",
            "PG": "8 +",
            "TV-PG": "8 +",
            "PG-13": "13 +",
            "TV-14": "14 +",
            "R": "17 +",
            "TV-MA": "18 +",
        },
        inplace=True,
    )

    # Normalize 'duration'
    chunk["normalize_of_duration"] = normalize(chunk[["duration"]])

    # Standardize 'duration'
    scaler = StandardScaler()
    chunk["standardize_of_duration"] = scaler.fit_transform(chunk[["duration"]])
    
    return chunk

# Main script
if __name__ == "__main__":
    # Read the CSV file
    file = pd.read_csv(
        "Netflix_Movies_and_TV_Shows.csv",
        names=["title", "type", "genre", "year", "rating_for_ages", "duration", "country"],
        header=0,
        dtype={"year": np.int16},
    )

    file.to_json("Netflix_Movies_and_TV_Shows.json", orient="records")
    movies = pd.read_json("Netflix_Movies_and_TV_Shows.json")
    movies.set_index("title", inplace=True)

    # Split DataFrame into chunks
    num_chunks = cpu_count()  # Number of processes
    chunks = np.array_split(movies, num_chunks)

    # Use multiprocessing to process chunks
    with Pool(num_chunks) as pool:
        results = pool.map(process_chunk, chunks)

    # Combine processed chunks
    processed_movies = pd.concat(results)
    
    print(processed_movies)

    get_result= requests.get("https://raw.githubusercontent.com/zeidg/project/main/Netflix_Movies_and_TV_Shows.json")
    if get_result:
        print("Request was successful")
        print(get_result.status_code)
        print(get_result.url)
        print(get_result.text)
    else:
        print("Request was not successful")