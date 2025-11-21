import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio
import os
from dotenv import load_dotenv

load_dotenv()

INSTANCE = os.getenv("INSTANCE")
REQUESTS_PER_SECOND = 2
CONCURRENT_USERS = 5
OUTPUT_CSV = f"data/lists_names.csv"
INPUT_CSV = f"data/letterboxd_members.csv"
SAVE_EVERY = 5  # save after every 5 users


async def fetch(session, url):
    async with session.get(url) as response:
        await asyncio.sleep(1 / REQUESTS_PER_SECOND)
        return await response.text()


def extract_listname(list_url):
    """
    Converts /deathproof/list/my-vhs-collection/ → "my-vhs-collection"
    """
    return list_url.strip("/").split("/")[-1]


async def scrape_user_lists(session, username):
    lists = []
    page = 1

    while True:
        url = f"https://letterboxd.com/{username}/lists/page/{page}/"
        html = await fetch(session, url)
        soup = BeautifulSoup(html, "html.parser")

        list_items = soup.select("div.listitem.js-listitem")
        if not list_items:
            break  # no more pages

        for item in list_items:
            # H2 block with title + link
            title_tag = item.select_one("h2.name.prettify a")
            if not title_tag:
                continue

            list_title = title_tag.text.strip()
            list_url = title_tag.get("href")
            listname = extract_listname(list_url)

            lists.append({
                "username": username,
                "listname": listname,
                "title": list_title
            })

        page += 1

    return lists


async def main():
    print("Running instance:", INSTANCE)

    df_users = pd.read_csv(INPUT_CSV)
    usernames = df_users["username"].tolist()

    all_lists = []

    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENT_USERS)
    async with aiohttp.ClientSession(connector=connector) as session:
        for idx, username in enumerate(tqdm_asyncio(usernames, desc="Users"), 1):
            try:
                user_lists = await scrape_user_lists(session, username)
                all_lists.extend(user_lists)
            except Exception as e:
                print(f"Error scraping {username}: {e}")

            # save batch
            if idx % SAVE_EVERY == 0:
                df = pd.DataFrame(all_lists)
                df.to_csv(
                    OUTPUT_CSV,
                    mode="a",
                    index=False,
                    header=not os.path.exists(OUTPUT_CSV)
                )
                all_lists = []

        # save remaining
        if all_lists:
            df = pd.DataFrame(all_lists)
            df.to_csv(
                OUTPUT_CSV,
                mode="a",
                index=False,
                header=not os.path.exists(OUTPUT_CSV)
            )

    print(f"All lists saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
