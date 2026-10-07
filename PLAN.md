# План завершення imdb-parser

Дата: 2026-10-07. Початковий огляд: `bab262b`; реалізація: branch `codex/complete-cli`.

## Мета та межі

Погоджена мета: надійний CLI для імпорту й оновлення `title.basics` та `title.ratings` у SQLite.

Готовий інструмент правильно переносить значення IMDb, підтримує повторний запуск, обробляє збої та працює з великими файлами без завантаження всього dataset у RAM. Поточні прапорці `-d`, `-u`, `-p`, `-t`, `-a` залишаються доступними. `-a` не очищає таблицю.

Нові datasets, API, UI та автоматичний scheduler не входять у цей план. Імпорт додає й оновлює записи; відсутність запису в наступному dataset не означає його видалення. Ratings оновлюються для наявних titles; unmatched ratings рахуються окремо, без створення неповних titles.

Основна база за уточненням користувача від 2026-10-07 — `db/imdb.sqlite`. SQLite CLI, parser, upsert, rollback, process lock та безпечні file operations реалізовано. Suite перевіряє 36 сценаріїв у 10 test files. Перший повний import пройшов; повторний і фінальне оформлення ще тривають. MySQL configuration доступна як попередній backend. Поточне середовище: PHP 8.2.34 у `.tools/php-8.2/`, SQLite 3.53.4, DBAL 2.13.9. Composer requirement — `^8.2`; `run.bat` обирає локальний runtime, PhpStorm language level — 8.2. Локальний `composer.lock` синхронізовано; він залишається untracked. PHP 8.2 lint, platform requirements, SQLite integration test з `E_ALL`, project launcher та HTTPS HEAD до IMDb пройшли. Попередні PDO deprecation notices усунуто в перевірених сценаріях. Composer попереджає про abandoned `doctrine/cache`; зміна major version DBAL виходить за цю доробку й залишається окремим рішенням.

## Початковий огляд MySQL backend

Місця й висновки цієї таблиці відповідають версії `bab262b`. SQLite використовує окрему схему з integer years, nullable numeric fields і TEXT titles.

| Місце | Тригер і наслідок | Заплановане рішення |
| --- | --- | --- |
| `run.php:158–176` | `YEAR` не зберігає рік `1894`, який є в першому локальному записі. У strict mode це помилка; без нього можливий `0000`. | Використати nullable integer для років. |
| `run.php:199–210` | `\N` передається до SQL без нормалізації. Відсутні значення конфліктують із числовими колонками `NOT NULL`. | Перетворювати `\N` на SQL `NULL`; узгодити nullable колонки. |
| `run.php:169–170, 209` | INSERT із basics не містить ratings, а схема вимагає `averageRating` і `numVotes` без defaults. У strict mode новий title може не вставитися. | Відсутній rating зберігати як `NULL`, а не вигаданий нуль. |
| `run.php:191` | Read-only проба підтвердила: один TSV рядок довший за 1000 bytes читається як два записи. | Читати повні TSV рядки без цього обмеження. |
| `run.php:204–209` | UPDATE без змін може повернути нуль, після чого виконується INSERT із тим самим primary key. | Атомарний upsert за `tconst`. |
| `run.php:188–229` | Транзакція відкривається до перевірки файлу; явного rollback при помилці немає. | Перевірити файл до транзакції; rollback поточного batch і закриття ресурсів. |
| `run.php:37–39, 135–176` | Після показу help виконання продовжується до створення каталогу й таблиці. Також `-d` та `-u` доходять до DDL. | Завершувати help одразу; звертатися до MySQL лише для операцій із базою. |
| `run.php:67–83` | Download пише прямо в кінцевий файл. Збій може знищити попередню справну копію; таймаути не задані. | Тимчасовий файл, явні таймаути, перевірка результату та безпечна заміна. |
| `composer.json`, tracked files | PHP minimum нижчий за вимогу встановленого DBAL; project tests і `.gitignore` відсутні. | Узгодити вимоги, додати ізольовані tests та правила для локальних файлів. |

Початкові проби не запускали importer і не підключалися до MySQL. Після переходу на SQLite додано ізольовані tests. Результати повного dataset наведено у `docs/import-benchmark.md`.

## Порядок виконання

### 1. Підготувати відтворювані перевірки

Файли: `composer.json`, `.gitignore`, `tests/run.php`, `tests/*Test.php`, `tests/fixtures/`.

- [x] Узгодити PHP minimum з DBAL і явно вказати потрібні extensions, зокрема PDO SQLite. Вимоги PHP `^8.2`, DBAL `^2.13.9`; platform check пройшов.
- [x] Визначити підтримувані PHP та SQLite versions; зафіксувати їх у README. Перевірити SQLite version на окремій test database.
- [x] Визначити політику `composer.lock` для цього CLI. Перевірити локальний lockfile перед включенням; не додавати його автоматично.
- [x] Ігнорувати `config.php`, `vendor/`, `exchange/`, `.idea/`, локальний `composer.phar` і тимчасові файли.
- [x] Додати простий PHP test runner із явними перевірками, які не залежать від налаштування `assert`. Зайві runtime dependencies не додавати.
- [x] Створити власні малі fixtures: Unicode, quotes, `\N`, рік `1894`, довгий title, повторний `tconst`, відсутній rating і пошкоджений рядок.
- [x] SQLite tests використовують власні тимчасові файли. Runner не використовує робочий `config.php`; можливі MySQL tests потребують окремих test credentials та opt-in.

Готово, коли unit tests запускаються без мережі та MySQL, а integration tests фізично відокремлені від робочих даних.

### 2. Виправити TSV parser і схему

Файли: `run.php`, мінімальний helper `lib/importer.php`, `tests/ParserTest.php`, `tests/ImportTest.php`.

- [x] Читати повний рядок, зберігати literal quotes, Unicode та порожні кінцеві поля. Перевіряти потрібні headers, дублікати headers і кількість колонок.
- [x] Нормалізувати `\N` в `NULL`. Перевіряти числові значення до SQL. Некоректний запис зупиняє імпорт із назвою файлу та номером рядка.
- [x] Зберігати роки як nullable integer; nullable numeric fields і ratings не підміняти нулями. Для titles прибрати обмеження 255 characters без обрізання даних.
- [x] Зберегти таблицю `title`, ключ `tconst`, назви колонок і потрібні індекси. Не вводити нову модель каталогу.
- [x] Перед зміною наявної SQLite schema перевірити сумісність. Якщо потрібна зміна колонок, надати окрему явну міграцію й описати backup. `CREATE TABLE IF NOT EXISTS` не замінює міграцію.

Готово, коли fixtures імпортуються у SQLite, `1894` зберігається точно, missing values є `NULL`, довгі назви не обрізаються, а зміни схеми зберігають наявні записи.

### 3. Зробити повторний імпорт безпечним

Файли: `run.php`, `lib/importer.php`, `tests/ImportTest.php`.

- [x] Замінити UPDATE-then-INSERT на параметризований upsert для basics. SQL має відповідати підтримуваній версії SQLite.
- [x] Оновлювати ratings лише для наявних `tconst`; рахувати unmatched entries.
- [x] Перевіряти файли й headers до запису. `TRANSACTION_PORTION` має бути додатним integer; headers не входять до лічильника даних.
- [x] Commit виконується після batch. При помилці rollback охоплює поточний batch. Раніше committed batches залишаються в базі; це явно вказується в результаті.
- [x] Закривати файли й з'єднання через гарантоване cleanup. Після збою дозволити повторний запуск із початку без duplicate-key errors; окремі checkpoints поки не потрібні.

Готово, коли два імпорти однакових fixtures не створюють duplicates, змінені дані оновлюються, а збій усередині batch не залишає його частково записаним.

### 4. Усунути побічні дії CLI

Файли: `run.php`, `run.bat`, `config.php.example`, `tests/CliTest.php`.

- [x] Help і запуск без arguments завершуються до читання credentials, створення файлів та звернення до MySQL.
- [x] Невідомий argument дає зрозумілу помилку й ненульовий exit code.
- [x] `-d` та `-u` не відкривають SQLite і не змінюють schema. `-p` перевіряє потрібні файли. `-t` очищає таблицю лише при явному передаванні цього прапорця.
- [x] Нормалізувати `DOWNLOAD_DIR`, включно зі шляхами без кінцевого slash і Windows paths із пробілами.
- [x] Повертати exit code 0 при успіху; при помилці писати її в stderr і повертати ненульовий code. Паролі не потрапляють у повідомлення.
- [x] Додати один локальний process lock для операцій у спільному робочому каталозі; другий процес завершується до змін. Це не є distributed lock між різними хостами.

Готово, коли поведінку всіх прапорців підтверджено subprocess tests, а help не має побічних дій.

### 5. Захистити download та extraction від збоїв

Файли: `run.php`, `config.php.example`, `tests/DownloadTest.php`.

- [x] Завантажувати в тимчасовий файл у тому самому каталозі. Замінювати попередню копію лише після успішного завершення й перевірки gzip.
- [x] Додати настроювані connection і transfer timeouts. Обмежити retries тимчасових network/HTTP errors; постійні помилки завершують операцію.
- [x] Перевіряти open, read і write; обробляти також частковий write. Завжди закривати handles.
- [x] Extraction теж пише в тимчасовий файл і зберігає попередній TSV при помилці. Перевірка gzip не повинна покладатися лише на file extension.
- [x] Перевірити локальним HTTP fixture: 404, 500, timeout, обірваний download, пошкоджений gzip та недоступний каталог.

Готово, коли жоден із цих збоїв не замінює справний кінцевий файл частковою копією.

### 6. Перевірити швидкість і пам'ять

Файли: `lib/importer.php`, `tests/ImportTest.php`, `docs/import-benchmark.md`.

- [x] Виміряти rows/sec, SQL query count, elapsed time і peak memory на однаковому наборі даних та окремій локальній SQLite database.
- [x] Повторно використовувати prepared statements. Якщо SQL round trips є головним обмеженням, додати bounded batch writes без завантаження dataset у RAM.
- [x] Обмежувати batch за rows і bytes з урахуванням SQLite limit на кількість SQL parameters; перевірити неповний останній batch.
- [x] Порівняти baseline та результат на одному середовищі. Використати фактичне порівняння без абсолютного rows/sec SLA; окремого числового target користувач не задав.
- [ ] Виконати повний import локальних datasets і повторний запуск. Зафіксувати versions, file sizes, counts, час і пам'ять.

Готово, коли використання RAM залежить від розміру рядка та batch, а не від всього файла, і є звіт із реальним повним запуском.

### 7. Оформити завершення

Файли: `README.md`, `AGENTS.md`, `config.php.example`, цей план.

- [x] Описати встановлення, requirements, прапорці, test commands, schema migration та правила повторного запуску після збою.
- [x] Описати відмінність між batch rollback і rollback усього import, а також поведінку unmatched ratings.
- [x] Додати підсумок CLI: processed rows, unmatched ratings, elapsed time, peak memory та фінальний статус. Лічильники мають відображати фактичні результати.
- [x] Перевірити всі документовані commands на чистій локальній копії.
- [x] Для кожного завершеного етапу виконати відповідні checks і зробити окремий локальний commit лише потрібних файлів. Push і pull request потребують окремого погодження.

## Критерії готовності проєкту

- [x] Чиста інсталяція відтворюється за README.
- [x] Unit та integration tests проходять на задокументованих versions.
- [ ] Повний import і повторний import завершуються успішно без duplicates та втрати значень.
- [x] Bad input, database error і download failure дають коректний exit code; ресурси закриті, поточний batch відкочено.
- [x] Help і filesystem-only operations не змінюють базу.
- [x] Якщо schema наявної SQLite бази змінюється, є явна перевірена міграція.
- [ ] Є вимірювання швидкості й peak memory на повному dataset.

## Джерела і межі перевірки

- [IMDb dataset specification](https://data.imdb.com/non-commercial-datasets/): TSV headers, UTF-8, missing-value marker `\N`, поля basics і ratings. Ці datasets мають умови personal/non-commercial use; посилання потрібно зберегти в README.
- [MySQL YEAR documentation](https://dev.mysql.com/doc/refman/8.4/en/year.html): допустимі роки `1901–2155` та `0000`; поведінка при invalid values залежить від strict SQL mode.
- Локально перевірено source, tracked files, вимогу PHP у встановленому DBAL, headers і по одному рядку gzip datasets. Повний import під час планування не перевірявся. SQLite integration test додано після зміни backend; MySQL SQL mode не є вимогою основного SQLite import.

Етап 2: SQLite schema не змінюється. PRAGMA перевіряє types, nullability і primary key перед DDL; несумісна схема потребує окремої міграції. Parser tests: 6 сценаріїв RED → GREEN.
## Виконання та межі

Завершені етапи мають локальні commits: harness `c671397`, parser `6e5712e`, idempotence/rollback `c7a4e80`, CLI `5ffd842`, file operations `c250f48`, bounded writes `ff8014c`. Після незалежного review додано явні NULL ratings для старих DEFAULT 0 schemas та відмову від composite primary key до clearing (`aa5bfd6`). Windows test runner зберігає повний redirected output (`8167cc0`).

Перевірено чисте Composer install без локального config/vendor/lock, усі test files, launcher/help, lint, Composer validation/platform requirements та обидва benchmark commands. CLI flags перевірено в ізольованих subprocess tests. Working database не використовується для tests.

| Межа | Рішення та наслідок |
| --- | --- |
| MySQL | Залишається legacy; потребує окремої schema та integration validation. |
| Process lock | Захищає один checkout; інші writers потребують зовнішньої координації. |
| SQLite durability | Збережено defaults; звичайні failure/retry перевірено. Hardware power loss та особливості інших filesystems потребують окремих перевірок. |
| Gzip | Підтримано один member; concatenated streams відхиляються. |
| Runtime | Результати підтверджено для PHP 8.2.34 x64 / SQLite 3.53.4; інші runtimes окремо не перевірено. |
| Dataset | Повні перевірки використовують наявні local snapshots; їхню актуальність не заявлено. |
| Review | Reviewer перевірив source і tests; повний/repeat import та чисте встановлення перевіряє виконавець. |

Відкладений Minor: якщо OS блокує unlink, `.part` може залишитися без повідомлення про residual path. Справний кінцевий файл зберігається; це обмеження описано в README.