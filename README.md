
# Star Wars Async Loader

Асинхронная загрузка персонажей из [SWAPI](https://swapi.tech/) в PostgreSQL с использованием `aiohttp`, `SQLAlchemy` и `asyncio`.

## 📦 Требования

- Python 3.10+
- PostgreSQL (созданная база `starwars` и пользователь `sw_user` с паролем `1234`)

## 🚀 Установка и запуск

1. **Клонируйте репозиторий**  
   ```bash
   git clone https://github.com/bsekinaev/async_homework.git
   cd async_homework
   ```

2. **Создайте виртуальное окружение и установите зависимости**  
   ```bash
   python -m venv venv
   source venv/bin/activate      # Linux/Mac
   # или
   venv\Scripts\activate         # Windows
   pip install -r requirements.txt
   ```

3. **Настройте базу данных PostgreSQL**  
   - Убедитесь, что сервер PostgreSQL запущен.
   - Создайте базу данных `starwars` и пользователя `sw_user` (или измените параметры подключения в `migrate.py` и `load_data.py`).

4. **Выполните миграцию**  
   ```bash
   python migrate.py
   ```

5. **Запустите загрузку данных**  
   ```bash
   python load_data.py
   ```

## 📊 Результат

- В базу данных загружены все 82 персонажа.
- Поля `homeworld`, `films`, `species`, `starships`, `vehicles` содержат названия (не URL).
- Повторный запуск не создаёт дубликатов (используется `ON CONFLICT DO UPDATE`).

## 🛠 Используемые технологии

- `aiohttp` – асинхронные HTTP-запросы
- `SQLAlchemy` – ORM для работы с PostgreSQL
- `asyncpg` – асинхронный драйвер для PostgreSQL
- `tenacity` – повторные попытки при сетевых ошибках

## 📁 Структура проекта

```
├── migrate.py          # создание таблицы
├── load_data.py        # асинхронная загрузка
├── requirements.txt    # зависимости
└── README.md           # этот файл
```


## 📄 Лицензия

Учебный проект в рамках расширенного курса Python-разработчик от Нетологии.
