@echo off
setlocal
set "IMDB_PHP=%~dp0.tools\php-8.2\php.exe"
if not exist "%IMDB_PHP%" set "IMDB_PHP=php"
"%IMDB_PHP%" "%~dp0run.php" %*
exit /b %errorlevel%
