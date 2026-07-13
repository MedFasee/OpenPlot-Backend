@echo off
setlocal enabledelayedexpansion

set PGPASSWORD=postgres
set PGHOST=localhost
set PGPORT=5432
set PGDATABASE=postgres
set PGUSER=postgres

REM Opcional: descomente se quiser evitar pedir senha a cada dia
REM set PGPASSWORD=postgres

set SCRIPT_DIR=%~dp0
set SQL_FILE=%SCRIPT_DIR%02_copy_one_day.sql

set START_DATE=2025-09-29
set END_DATE=2026-06-07

echo Iniciando copia por dia para openplot.measurements_new...
echo Database: %PGDATABASE%
echo Inicio..: %START_DATE%
echo Fim.....: %END_DATE%
echo.

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$start = [datetime]'%START_DATE%';" ^
  "$end = [datetime]'%END_DATE%';" ^
  "while ($start -lt $end) {" ^
  "  $next = $start.AddDays(1);" ^
  "  $from = $start.ToString('yyyy-MM-ddT00:00:00Z');" ^
  "  $to = $next.ToString('yyyy-MM-ddT00:00:00Z');" ^
  "  Write-Host ('Copiando ' + $from + ' ate ' + $to);" ^
  "  & psql -h '%PGHOST%' -p '%PGPORT%' -U '%PGUSER%' -d '%PGDATABASE%' -v from_utc=$from -v to_utc=$to -f '%SQL_FILE%';" ^
  "  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }" ^
  "  $start = $next;" ^
  "}"

if errorlevel 1 (
    echo.
    echo Falha durante a copia.
    exit /b 1
)

echo.
echo Copia por dia concluida.