import asyncio
import aiohttp
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert
from migrate import characters_table
from typing import Dict, List

DATABASE_URL = 'postgresql+asyncpg://sw_user:1234@localhost/starwars'
BASE_API_URL = 'https://www.swapi.tech/api'
REQUEST_TIMEOUT = 10
CONCURRENT_REQUESTS = 20

semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

async def fetch_json(session: aiohttp.ClientSession, url: str) -> Dict:
    async with semaphore:
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            resp.raise_for_status()
            return await resp.json()



async def get_all_people_uids(session: aiohttp.ClientSession) -> List[int]:
    uids = []
    page = 1
    while True:
        url = f'{BASE_API_URL}/people?page={page}&limit=100'
        data = await fetch_json(session, url)
        results = data.get('results',[])
        if not results:
            break
        for item in results:
            uid = item.get('uid')
            if uid:
                uids.append(int(uid))
        total_pages = data.get('total_pages', 0)
        if page >= total_pages:
            break
        page += 1
    return uids

async def fetch_character(session: aiohttp.ClientSession, uid: int) -> Dict:
    url = f'{BASE_API_URL}/people/{uid}'
    data = await fetch_json(session, url)
    properties = data.get('result', {}).get('properties',{})
    properties['uid'] = uid
    return properties

async def fetch_planet_name(session: aiohttp.ClientSession, planet_url: str) -> str:
    data = await fetch_json(session, planet_url)
    return data.get("result", {}).get('properties', {}).get('name','unknown')

async def load_all_characters():
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with aiohttp.ClientSession() as session:
        print('Получение списка персонажей')
        uids = await get_all_people_uids(session)
        print(f'Найдено {len(uids)} персонажей')

        print('Получение сведений о персонаже')
        characters_raw = await asyncio.gather(*[fetch_character(session, uid) for uid in uids])

        homeworld_urls = set()
        for ch in characters_raw:
            hw_url = ch.get('homeworld')
            if hw_url:
                homeworld_urls.add(hw_url)

        print(f'Найдено {len(homeworld_urls)} уникальных URL-адресов планет. Поиск названий планет...')
        planet_cache = {}
        tasks = [fetch_planet_name(session,url) for url in homeworld_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for url, result in zip(homeworld_urls, results):
            if isinstance(result, Exception):
                print(f'Не удалось получить название планеты {url}:{result}')
                planet_cache[url] = 'unknown'
            else:
                planet_cache[url] = result

        records = []
        for ch in characters_raw:
            hw_url = ch.get('homeworld')
            planet_name = planet_cache.get(hw_url, 'unknown') if hw_url else None
            record = {
                'id': ch['uid'],
                'name':ch.get('name'),
                'birth_year': ch.get('birth_year'),
                'eye_color': ch.get('eye_color'),
                'gender': ch.get('gender'),
                'hair_color': ch.get('hair_color'),
                'skin_color': ch.get('skin_color'),
                'mass': ch.get('mass'),
                'homeworld': planet_name,
            }
            records.append(record)

        async with async_session() as db_session:
            async with db_session.begin():
                for rec in records:
                    stmt = insert(characters_table).values(rec)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['id'],
                        set_={k: v for k, v in rec.items() if k != 'id'}
                    )
                    await db_session.execute(stmt)
            print(f'Сохранено {len(records)} персонажей в базу данных')
    await engine.dispose()

if __name__=='__main__':
    asyncio.run(load_all_characters())