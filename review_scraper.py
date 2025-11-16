import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio
import re
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Rate limiting: max requests per second
INSTANCE = os.getenv("INSTANCE")
REQUESTS_PER_SECOND = 2
CONCURRENT_USERS = 5  # Number of users processed concurrently
OUTPUT_CSV = f"data/reviews_{INSTANCE}.csv"
INPUT_CSV = f"data/members_{INSTANCE}.csv"
SAVE_EVERY = 5  # save after every 5 users

# Convert star rating like ★★★½ to number
def parse_rating(star_text):
    star_text = star_text.strip()
    if not star_text:
        return None
    full_stars = star_text.count('★')
    half_star = 0.5 if '½' in star_text else 0
    return full_stars + half_star

async def fetch(session, url):
    async with session.get(url) as response:
        await asyncio.sleep(1 / REQUESTS_PER_SECOND)  # rate limit
        return await response.text()

async def scrape_user_reviews(session, username):
    reviews = []
    page = 1
    while True:
        url = f"https://letterboxd.com/{username}/reviews/films/page/{page}/"
        html = await fetch(session, url)
        soup = BeautifulSoup(html, "html.parser")
        review_items = soup.select("div.listitem.js-listitem")
        if not review_items:
            break  # no more reviews

        for item in review_items:
            figure_div = item.find("div", class_="react-component figure")
            moviename = figure_div.get("data-item-slug") if figure_div else None
            title_tag = item.select_one("h2.name.-primary.prettify a")
            movietitle = title_tag.text.strip() if title_tag else None
            release_tag = item.select_one("span.releasedate a")
            releaseDate = release_tag.text.strip() if release_tag else None
            timestamp_tag = item.select_one("time.timestamp")
            date = timestamp_tag.get("datetime") if timestamp_tag else None
            rating_tag = item.select_one("span.rating")
            rating = parse_rating(rating_tag.text) if rating_tag else None
            review_tag = item.select_one("div.js-review-body")
            review = review_tag.get_text(strip=True) if review_tag else None

            reviews.append({
                "username": username,
                "moviename": moviename,
                "movietitle": movietitle,
                "releaseDate": releaseDate,
                "date": date,
                "rating": rating,
                "review": review
            })
        page += 1
    return reviews

async def main():
    print("Running instance",INSTANCE)
    df_users = pd.read_csv(INPUT_CSV)
    usernames = df_users['username'].tolist()
    all_reviews = []

    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENT_USERS)
    async with aiohttp.ClientSession(connector=connector) as session:
        for idx, username in enumerate(tqdm_asyncio(usernames, desc="Users"), 1):
            try:
                user_reviews = await scrape_user_reviews(session, username)
                all_reviews.extend(user_reviews)
            except Exception as e:
                print(f"Error scraping {username}: {e}")

            # Save every SAVE_EVERY users
            if idx % SAVE_EVERY == 0:
                df = pd.DataFrame(all_reviews)
                # Append mode: write header only if file doesn't exist
                df.to_csv(OUTPUT_CSV, mode='a', index=False, header=not os.path.exists(OUTPUT_CSV))
                all_reviews = []  # reset buffer

        # Save any remaining reviews
        if all_reviews:
            df = pd.DataFrame(all_reviews)
            df.to_csv(OUTPUT_CSV, mode='a', index=False, header=not os.path.exists(OUTPUT_CSV))

    print(f"All reviews saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
