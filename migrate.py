import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import Column, Integer, String, MetaData, Table

DATABASE_URL = 'postgresql+asyncpg://sw_user:1234@localhost/starwars'

metadata = MetaData()

characters_table = Table(
    'characters',
    metadata,
    Column('id',Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('birth_year', String),
    Column('eye_color', String),
    Column('gender', String),
    Column('hair_color', String),
    Column('skin_color', String),
    Column('mass', String),
    Column('homeworld', String),
    Column('films', String),
    Column('species', String),
    Column('starships', String),
    Column('vehicles', String),
)

async def run_migration():
    engine = create_async_engine(DATABASE_URL, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    await engine.dispose()
    print('Миграция выполнена')

if __name__=='__main__':
    asyncio.run(run_migration())