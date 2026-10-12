# Wikidata country enrichment

Дата: 2026-10-12. Repository: imdb-parser. Runtime: PHP 8.2, SQLite.
Статус: дизайн для перегляду; product code та working database ще не змінювалися.

## Мета та погоджені межі

IMDb залишається джерелом basics і ratings. З Wikidata отримуємо лише країни походження через IMDb ID (P345) та country of origin (P495). Країни використовуємо для SQL-вибірки й виключення індійських фільмів і серіалів під час import. Попередній відбір released movies та ended tvSeries/tvMiniSeries залишається.

За замовчуванням виключаємо title, якщо серед його країн є Індія, включно зі спільними виробництвами. Невідома країна не означає, що title не індійський: такі записи зберігаємо й рахуємо окремо. Це припущення для підтвердження під час перегляду.

Наявні індійські titles автоматично не видаляємо: користувач попросив фільтр при додаванні. Очищення наявних даних потребуватиме окремого конкретного запиту. Визначення країни за назвою, мовою або IMDb akas.region не використовуємо. Повний перехід на Wikidata, інші metadata, UI та scheduler не входять у зміну.

## Спосіб отримання

Рекомендований варіант — послідовні SPARQL requests за невеликими batches IMDb IDs і persistent SQLite cache. Повний Wikidata dump потребує значного download/storage; для цього доповнення він зайвий.

Query використовує VALUES із перевіреними tconst, P345 для зіставлення та OPTIONAL P495/P297. P297 дає допоміжний ISO alpha-2 code. Країну зберігаємо за її QID, навіть якщо ISO code відсутній. Дані кількох країн об'єднуємо без дублікатів. India визначається за Q668; code IN також має відповідати виключенню.

Початковий batch size — 100 IDs; паралельних HTTP requests немає. Cache refresh interval — 30 днів для відомих і невідомих результатів. Invalid JSON, неповна відповідь, timeout або HTTP error не стають результатом «країна невідома».

## Storage та migration

Додаємо до основної SQLite дві окремі таблиці, без зміни колонок title:

- title_country: tconst, country_qid, nullable country_code; primary key (tconst, country_qid), index для вибірки за країною.
- country_lookup: tconst як primary key та checked_at у UTC. Запис lookup без country rows означає успішно перевірений unknown result.

Cache може містити IDs, яких немає в title: зокрема виключені індійські titles або ще не імпортовані записи. Тому foreign key до title не потрібен. SQL-вибірка країн завжди приєднує cache до title.

Migration має окрему явну CLI action --migrate-countries. Для наявної SQLite спочатку створюємо consistent backup через VACUUM INTO та перевіряємо quick_check. Обидві таблиці й indexes створюємо транзакційно; існуючий title і ratings не змінюємо. Country mode для legacy MySQL відхиляємо до запису.

## CLI та workflow

Нова action --countries заповнює або оновлює cache для titles у поточній базі. Для першої live перевірки підтримуємо --country-limit=N з додатним integer; повторний запуск пропускає fresh cache і продовжує після failure.

Для -p/-a з country mode:

1. Перевірити config, TSV headers, process lock та country schema.
2. Потоково пройти eligible basics IDs і заповнити missing/stale cache. HTTP виконується поза title transactions.
3. Повторно потоково імпортувати basics, застосовуючи completion і country filters до SQL writes.
4. Оновити ratings лише для titles, які проходять ці фільтри.

Два проходи зберігають bounded memory і відокремлюють network failures від title writes. При -t -p country preflight має завершитися до clearing. -t не стирає reusable country cache.

Config: WIKIDATA_COUNTRIES_ENABLED, EXCLUDE_COUNTRIES (default [IN]), WIKIDATA_REFRESH_DAYS (default 30) та WIKIDATA_ENDPOINT. У попередніх configs country mode залишається disabled до явної migration та activation; після погодженої реалізації local config буде активовано. Endpoint за замовчуванням https://query.wikidata.org/sparql; credentials у URL не підтримуються. Tests використовують лише loopback fixture server.

## Failure handling та counters

HTTP має явний User-Agent, connect timeout 10 s, read timeout 60 s, response limit 2 MiB і максимум 2 retries для transient failures. HTTP 429 враховує Retry-After; процес зупиняється після вичерпання retries. Звичайні requests виконуються послідовно.

Усі rows відповіді перевіряємо перед cache commit: requested IMDb IDs, country entity URIs і ISO codes. Відповідь має охоплювати кожен запитаний ID, включно з unknown results. Country rows і checked_at замінюємо одним атомарним batch. Попередній cache зберігається при помилці.

Summary розділяє checked IDs, country-known/unknown IDs, skipped-by-country basics та unmatched ratings. Unknown показує неповноту відбору; помилка сервісу дає nonzero exit. Раніше committed cache batches зберігаються й повторно використовуються.

## Перевірка та readiness

Ізольовані integration tests мають покрити:

- India, co-production, multiple countries та historical country без ISO code;
- unknown ID/no country; duplicate rows; response з чужим або пропущеним ID;
- invalid JSON, oversized body, HTTP 429/retry, timeout та permanent errors;
- fresh/expired cache, повторний запуск і atomic replacement/rollback;
- country filtering до basics upsert та відповідний ratings filter;
- completion filter, preflight перед -t, help/download-only без database side effects.

Готово після проходження нового test file, повного suite й lint, explicit migration з backup та обмеженої live звірки з Wikidata. Повне заповнення 612 тисяч titles — окрема тривала фаза з видимими counters та можливістю повторного запуску; ETA без вимірювання не заявляємо.

Live read-only probe 2026-10-12: HTTP 200, valid SPARQL JSON, 0.508 s для 5 IDs. tt1187043, tt5074352 і tt8178634 дали Q668/IN; tt0133093 дав Q30/US та Q408/AU; контрольний unknown ID дав row без країни. Покриття всієї working database ще не виміряне.

## Джерела

- [P345: IMDb ID](https://www.wikidata.org/wiki/Property:P345)
- [P495: country of origin](https://www.wikidata.org/wiki/Property:P495)
- [P297: ISO alpha-2 code](https://www.wikidata.org/wiki/Property:P297)
- [Wikidata data access](https://www.wikidata.org/wiki/Wikidata:Data_access)
- [WDQS limits](https://www.mediawiki.org/wiki/Wikidata_Query_Service/Implementation)
