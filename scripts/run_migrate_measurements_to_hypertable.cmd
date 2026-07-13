@echo off
setlocal

rem Executor CMD para migracao full de openplot.measurements para hypertable.
rem Ajustar as variaveis abaixo antes de executar.

set "PGHOST=localhost"
set "PGPORT=5432"
set "PGDATABASE=postgres"
set "PGUSER=postgres"
set "PGPASSWORD=postgres"

set "SCRIPT_DIR=%~dp0"
set "SQL_FILE=%SCRIPT_DIR%migrate_measurements_to_hypertable.sql"
set "PSQL=psql -v ON_ERROR_STOP=1 -h %PGHOST% -p %PGPORT% -U %PGUSER% -d %PGDATABASE%"

if not exist "%SQL_FILE%" (
	echo Arquivo SQL nao encontrado: %SQL_FILE%
	exit /b 1
)

echo.
echo Iniciando migracao full de openplot.measurements para hypertable...
echo Host......: %PGHOST%
echo Porta.....: %PGPORT%
echo Database..: %PGDATABASE%
echo Usuario...: %PGUSER%
echo Script....: %SQL_FILE%
echo.
echo A tabela atual sera preservada como openplot.measurements_before_hypertable.
echo.

%PSQL% -f "%SQL_FILE%"
if errorlevel 1 goto :erro

echo.
echo Migracao executada com sucesso.
echo Validar consultas da aplicacao antes de remover o backup logico.
exit /b 0

:erro
echo.
echo Falha na migracao.
exit /b 1
