import aiohttp
import asyncio
import pandas as pd
import os
import json
from tqdm import tqdm

API_KEY = "daf21cc0eb352ea67de337de4f4e3d86"
BASE_URL = "https://api.themoviedb.org/3"

# ===========================================================
# CONFIG
# ===========================================================
INPUT_FILE = "data/Letterboxd_Movies.csv"          # Must contain: title, year
OUTPUT_FILE = "movies_tmdb.csv"    # Final output
CHECKPOINT_FILE = "checkpoint.json"
SAVE_EVERY = 1000                  # Save every N movies
MAX_REQUESTS_PER_SEC = 2           # Safe rate limit
# ===========================================================


sem = asyncio.Semaphore(MAX_REQUESTS_PER_SEC)


def load_checkpoint():
    """Load already processed movie indices."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done_set):
    """Save processed movie indices to disk."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(done_set), f)


async def fetch_json(session, url, params):
    """GET request with automatic 429 retry handling."""
    async with sem:
        async with session.get(url, params=params) as response:
            if response.status == 429:
                # Rate limited → wait then retry same request
                await asyncio.sleep(2)
                return await fetch_json(session, url, params)

            return await response.json()


async def tmdb_search(session, title, year):
    """Search TMDb for the movie ID."""
    params = {"api_key": API_KEY, "query": title}

    if not pd.isna(year):
        params["year"] = int(year)

    url = f"{BASE_URL}/search/movie"
    data = await fetch_json(session, url, params)

    results = data.get("results", [])
    if results:
        return results[0]["id"]

    return None


async def tmdb_details(session, tmdb_id):
    """Fetch full movie details including credits."""
    params = {
        "api_key": API_KEY,
        "append_to_response": "credits"
    }
    url = f"{BASE_URL}/movie/{tmdb_id}"
    return await fetch_json(session, url, params)


def extract_fields(data):
    """Extract required metadata from TMDb."""
    # Director(s)
    directors = []
    if "credits" in data:
        for c in data["credits"].get("crew", []):
            if c.get("job") == "Director":
                directors.append(c["name"])

    runtime = data.get("runtime")

    studios = [c["name"] for c in data.get("production_companies", [])]

    # "Publisher" doesn't exist for movies → use production companies
    publishers = studios

    genres = [g["name"] for g in data.get("genres", [])]

    plot = data.get("overview")

    poster_path = data.get("poster_path")
    poster_url = (
        f"https://image.tmdb.org/t/p/original{poster_path}"
        if poster_path else None
    )

    return {
        "director": ", ".join(directors),
        "runtime": runtime,
        "studio": ", ".join(studios),
        "publisher": ", ".join(publishers),
        "genre": ", ".join(genres),
        "plot": plot,
        "poster": poster_url,
    }


async def process_movie(idx, row, session, results, done_set):
    """Process a single movie: Search → Details → Extract."""
    title = row["m.title"]
    year = row["m.releaseDate"]

    tmdb_id = await tmdb_search(session, title, year)

    if not tmdb_id:
        results.append({
            "title": title,
            "year": year,
            "tmdb_id": None,
            "director": None,
            "runtime": None,
            "studio": None,
            "publisher": None,
            "genre": None,
            "plot": None,
            "poster": None,
        })
        done_set.add(idx)
        return

    info = await tmdb_details(session, tmdb_id)
    fields = extract_fields(info)

    results.append({
        "title": title,
        "year": year,
        "tmdb_id": tmdb_id,
        **fields
    })

    done_set.add(idx)


async def main():
    df = pd.read_csv(INPUT_FILE)
    done_set = load_checkpoint()

    # If previous run exists, load partial results
    results = []
    if os.path.exists(OUTPUT_FILE):
        existing = pd.read_csv(OUTPUT_FILE)
        results = existing.to_dict("records")

    async with aiohttp.ClientSession() as session:
        tasks = []
        counter = 0

        for idx, row in df.iterrows():
            if idx in done_set:
                continue

            tasks.append(process_movie(idx, row, session, results, done_set))

            # Batch execution every SAVE_EVERY movies
            if len(tasks) >= SAVE_EVERY:
                for t in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                    await t

                # Save progress
                pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
                save_checkpoint(done_set)
                tasks = []

        # Process leftover tasks
        if tasks:
            for t in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                await t
            pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
            save_checkpoint(done_set)


if __name__ == "__main__":
    asyncio.run(main())
