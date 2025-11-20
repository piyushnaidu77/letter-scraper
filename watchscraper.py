import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio
import os
import re
from dotenv import load_dotenv

load_dotenv()

INSTANCE = os.getenv("INSTANCE")
REQUESTS_PER_SECOND = 2
CONCURRENT_USERS = 5
OUTPUT_CSV = f"data/watched_{INSTANCE}.csv"
INPUT_CSV = f"data/letterboxd_members.csv"
SAVE_EVERY = 5  # save after every N users


def parse_rating(star_text):
    if not star_text:
        return None
    star_text = star_text.strip()
    full = star_text.count("★")
    half = 0.5 if "½" in star_text else 0
    return full + half


def parse_item_name(item_name):
    """
    Convert 'Eternity (2025)' → title='Eternity', year='2025'
    """
    if not item_name:
        return None, None

    match = re.match(r"(.+)\s+\((\d{4})\)", item_name)
    if match:
        return match.group(1).strip(), match.group(2)

    return item_name, None


async def fetch(session, url):
    async with session.get(url) as response:
        await asyncio.sleep(1 / REQUESTS_PER_SECOND)
        return await response.text()


async def scrape_user_films(session, username):
    films = []
    page = 1

    while True:
        url = f"https://letterboxd.com/{username}/films/page/{page}/"
        html = await fetch(session, url)
        soup = BeautifulSoup(html, "html.parser")

        # Find all film grid items
        items = soup.select("li.griditem")
        if not items:
            break  # no more pages

        for item in items:
            poster_div = item.select_one("div.react-component[data-item-slug]")
            if not poster_div:
                continue

            moviename = poster_div.get("data-item-slug")
            item_name = poster_div.get("data-item-name")
            movietitle, releaseDate = parse_item_name(item_name)

            rating_tag = item.select_one("p.poster-viewingdata span.rating")
            rating = parse_rating(rating_tag.text) if rating_tag else None

            films.append({
                "username": username,
                "moviename": moviename,
                "movietitle": movietitle,
                "releaseDate": releaseDate,
                "rating": rating
            })

        page += 1

    return films


async def main():
    print("Running instance", INSTANCE)

    df_users = pd.read_csv(INPUT_CSV)
    usernames = df_users["username"].tolist()

    all_films = []

    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENT_USERS)
    async with aiohttp.ClientSession(connector=connector) as session:
        for idx, username in enumerate(tqdm_asyncio(usernames, desc="Users"), 1):
            try:
                user_films = await scrape_user_films(session, username)
                all_films.extend(user_films)
            except Exception as e:
                print(f"Error scraping {username}: {e}")

            # Save every N users
            if idx % SAVE_EVERY == 0:
                df = pd.DataFrame(all_films)
                df.to_csv(
                    OUTPUT_CSV,
                    mode="a",
                    index=False,
                    header=not os.path.exists(OUTPUT_CSV)
                )
                all_films = []

        # Save remaining
        if all_films:
            df = pd.DataFrame(all_films)
            df.to_csv(
                OUTPUT_CSV,
                mode="a",
                index=False,
                header=not os.path.exists(OUTPUT_CSV)
            )

    print(f"All films saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
