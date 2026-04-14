import asyncio
import aiohttp
from typing import List, Dict, Set
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert
from migrate import characters_table

DATABASE_URL = 'postgresql+asyncpg://sw_user:1234@localhost/starwars'
BASE_API_URL = 'https://www.swapi.tech/api'
REQUEST_TIMEOUT = 10
CONCURRENT_REQUESTS = 20
MAX_RETRIES = 3

semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

planet_cache: Dict[str, str] = {}
film_cache: Dict[str, str] = {}
species_cache: Dict[str, str] = {}
starships_cache: Dict[str, str] = {}
vehicles_cache: Dict[str, str] = {}

@retry(
    stop = stop_after_attempt(MAX_RETRIES),
    wait = wait_exponential(multiplier=1, min=1, max=MAX_RETRIES),
    retry = retry_if_exception_type((aiohttp.ClientError,asyncio.TimeoutError))
)


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
        try:
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
        except Exception as e:
            print(f'Ошибка получения страницы {page}: {e}')
            break
    return uids

async def get_all_species(session: aiohttp.ClientSession) -> List[Dict]:
    species_list = []
    page = 1
    while True:
        url = f'{BASE_API_URL}/species?page={page}&limit=100'
        try:
            data = await fetch_json(session, url)
            results = data.get('results', [])
            if not results:
                break
            species_list.extend(results)
            total_pages = data.get('total_pages', 0)
            if page >= total_pages:
                break
            page += 1
        except Exception as e:
            print(f'Ошибка получения страницы {page} с видами: {e}')
            break
    return species_list


async def fetch_character(session: aiohttp.ClientSession, uid: int) -> Dict:
    url = f'{BASE_API_URL}/people/{uid}'
    try:
        data = await fetch_json(session, url)
        properties = data.get('result', {}).get('properties',{})
        properties['uid'] = uid
        return properties
    except Exception as e:
        print(f'Не удалось загрузить персонажа {uid}: {e}')
        return None

async def fetch_entity_name(session: aiohttp.ClientSession, url: str, cache: Dict[str, str]) -> str:
    if not url:
        return ''
    if url in cache:
        return cache[url]
    try:
        data = await fetch_json(session, url)
        props = data.get('result', {}).get('properties', {})
        if '/films/' in url:
            name = props.get('title', 'unknown')
        else:
            name = props.get('name', 'unknown')
        cache[url] = name
        return name
    except Exception as e:
        print(f'Ошибка при получении названия для {url}: {e}')
        cache[url] = 'unknown'
        return 'unknown'

async def fetch_list_names(session: aiohttp.ClientSession, urls: List[str], cache: Dict[str, str]) -> str:
    if not urls:
        return ''
    names = []
    for url in urls:
        name = await fetch_entity_name(session, url, cache)
        if name:
            names.append(name)
    return ','.join(names)


async def load_all_characters():
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with aiohttp.ClientSession() as session:
            print('Получение списка персонажей')
            uids = await get_all_people_uids(session)
            print(f'Найдено {len(uids)} персонажей')

            print('Получение сведений о персонаже')
            characters_raw = await asyncio.gather(*[fetch_character(session, uid) for uid in uids])
            characters_raw = [ch for ch in characters_raw if ch is not None]
            print(f'Успешно загружено {len(characters_raw)} персонажей')

            planet_urls: Set[str] = set()
            films_urls: Set[str] = set()
            species_urls: Set[str] = set()
            starships_urls: Set[str] = set()
            vehicles_urls: Set[str] = set()

            for ch in characters_raw:
                if ch.get('homeworld'):
                    planet_urls.add(ch['homeworld'])
                for film_url in ch.get('films', []):
                    films_urls.add(film_url)
                for species_url in ch.get('species', []):
                    species_urls.add(species_url)
                for starship_url in ch.get('starships', []):
                    starships_urls.add(starship_url)
                for vehicle_url in ch.get('vehicles', []):
                    vehicles_urls.add(vehicle_url)

            print(f'Найдено уникальных планет: {len(planet_urls)}')
            print(f'Найдено уникальных фильмов: {len(films_urls)}')
            print(f'Найдено уникальных рас: {len(species_urls)}')
            print(f'Найдено уникальных кораблей: {len(starships_urls)}')
            print(f'Найдено уникальных транспортных средств: {len(vehicles_urls)}')

            print('Загрузка названия планет')
            tasks = [fetch_entity_name(session, url, planet_cache) for url in planet_urls]
            await asyncio.gather(*tasks)

            print('Загрузка названий фильмов')
            tasks = [fetch_entity_name(session, url, film_cache) for url in films_urls]
            await asyncio.gather(*tasks)

            print('Загрузка названия рас')
            tasks = [fetch_entity_name(session, url, species_cache) for url in species_urls]
            await asyncio.gather(*tasks)

            print('Загрузка названий кораблей')
            tasks = [fetch_entity_name(session, url, starships_cache) for url in starships_urls]
            await asyncio.gather(*tasks)

            print('Загрузка названий транспорта')
            tasks = [fetch_entity_name(session, url, vehicles_cache) for url in vehicles_urls]
            await asyncio.gather(*tasks)


            records = []
            for ch in characters_raw:
                hw_url = ch.get('homeworld')
                planet_name = planet_cache.get(hw_url, 'unknown') if hw_url else None


                films_str = await fetch_list_names(session, ch.get('films', []), film_cache)
                species_str = await fetch_list_names(session, ch.get('species', []), species_cache)
                starships_str = await fetch_list_names(session, ch.get('starships', []), starships_cache)
                vehicles_str = await fetch_list_names(session, ch.get('vehicles', []), vehicles_cache)

                record = {
                    'id': ch['uid'],
                    'name': ch.get('name'),
                    'birth_year': ch.get('birth_year'),
                    'eye_color': ch.get('eye_color'),
                    'gender': ch.get('gender'),
                    'hair_color': ch.get('hair_color'),
                    'skin_color': ch.get('skin_color'),
                    'mass': ch.get('mass'),
                    'homeworld': planet_name,
                    'films': films_str,
                    'species': species_str,
                    'starships': starships_str,
                    'vehicles': vehicles_str,
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

    except Exception as e:
        print(f'Критическая ошибка {e}')
    finally:
        await engine.dispose()
        print('Соединение с бд закрыто')


if __name__=='__main__':
    asyncio.run(load_all_characters())