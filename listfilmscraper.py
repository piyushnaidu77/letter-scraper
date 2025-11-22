import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio
import re
import os
from dotenv import load_dotenv

load_dotenv()

INSTANCE = os.getenv("INSTANCE")
INPUT_CSV = f"data/lists_names.csv"
OUTPUT_CSV = f"data/list_films_{INSTANCE}.csv"

REQUESTS_PER_SECOND = 2
CONCURRENT = 5
SAVE_EVERY = 5


async def fetch(session, url):
    async with session.get(url) as response:
        await asyncio.sleep(1 / REQUESTS_PER_SECOND)
        return await response.text()


def parse_movie_title_and_year(frame_title):
    """
    e.g. "Friday the 13th Part 2 (1981)" → ("Friday the 13th Part 2", "1981")
    """
    if "(" in frame_title and frame_title.endswith(")"):
        parts = frame_title.rsplit("(", 1)
        title = parts[0].strip()
        year = parts[1].replace(")", "").strip()
        return title, year
    return frame_title, None


async def scrape_list_movies(session, username, listname):
    url = f"https://letterboxd.com/{username}/list/{listname}/"
    html = await fetch(session, url)
    soup = BeautifulSoup(html, "html.parser")

    films = []

    items = soup.select("li.posteritem.numbered-list-item")
    for li in items:
        # slug / ID
        comp = li.select_one("div.react-component")
        if not comp:
            continue

        slug = comp.get("data-item-slug")
        if not slug:
            continue

        frame_title_tag = comp.get("data-item-name")
        if not frame_title_tag:
            continue

        full_title = frame_title_tag.strip()
        movietitle, releaseDate = parse_movie_title_and_year(full_title)

        films.append({
            "username": username,
            "listname": listname,
            "moviename": slug,
            "movietitle": movietitle,
            "releaseDate": releaseDate
        })

    return films


async def main():
    df = pd.read_csv(INPUT_CSV)
    lists = df[["username", "listname"]].drop_duplicates()

    all_results = []

    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:

        for idx, row in enumerate(tqdm_asyncio(lists.itertuples(), desc="Lists"), 1):
            username = row.username
            listname = row.listname

            try:
                films = await scrape_list_movies(session, username, listname)
                all_results.extend(films)
            except Exception as e:
                print(f"Error scraping {username}/{listname}: {e}")

            # batch save
            if idx % SAVE_EVERY == 0:
                df_out = pd.DataFrame(all_results)
                df_out.to_csv(
                    OUTPUT_CSV,
                    mode="a",
                    index=False,
                    header=not os.path.exists(OUTPUT_CSV),
                )
                all_results = []

        # final flush
        if all_results:
            df_out = pd.DataFrame(all_results)
            df_out.to_csv(
                OUTPUT_CSV,
                mode="a",
                index=False,
                header=not os.path.exists(OUTPUT_CSV),
            )

    print("Done! Saved:", OUTPUT_CSV)


if __name__ == "__main__":
    asyncio.run(main())
