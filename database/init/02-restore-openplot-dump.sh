#!/usr/bin/env sh
set -eu

DUMP_FILE="/docker-entrypoint-dumps/openplot.dump"
FALLBACK_DUMP_FILE="/docker-entrypoint-dumps/openplot_antigo.dump"
RESTORE_ENABLED="${OPENPLOT_POSTGRES_RESTORE_DUMP:-true}"

run_pg_restore() {
  pg_restore \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --data-only \
    --disable-triggers \
    --no-owner \
    --no-privileges \
    -N _timescaledb_catalog \
    -N _timescaledb_cache \
    -N _timescaledb_config \
    -N _timescaledb_functions \
    -N _timescaledb_internal \
    -N timescaledb_information \
    -T openplot.measurements \
    --verbose \
    "$1"
}

if [ "$RESTORE_ENABLED" != "true" ]; then
  echo "[openplot-db-init] Dump restore desabilitado por OPENPLOT_POSTGRES_RESTORE_DUMP=$RESTORE_ENABLED"
  exit 0
fi

if [ ! -f "$DUMP_FILE" ]; then
  echo "[openplot-db-init] Dump nao encontrado em $DUMP_FILE; mantendo bootstrap SQL apenas."
  exit 0
fi

if [ ! -s "$DUMP_FILE" ]; then
  echo "[openplot-db-init] Dump em $DUMP_FILE esta vazio; mantendo bootstrap SQL apenas."
  exit 0
fi

echo "[openplot-db-init] Restaurando dump $DUMP_FILE em $POSTGRES_DB..."
BOM_HEX="$(dd if="$DUMP_FILE" bs=2 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n' || true)"
RESTORE_FILE="$DUMP_FILE"

if pg_restore --list "$RESTORE_FILE" >/tmp/openplot.pgrestore.list 2>/tmp/openplot.pgrestore.err; then
  echo "[openplot-db-init] Formato detectado: archive custom (pg_restore)."
  run_pg_restore "$RESTORE_FILE"
else
  if [ -f "$FALLBACK_DUMP_FILE" ] && [ -s "$FALLBACK_DUMP_FILE" ] && pg_restore --list "$FALLBACK_DUMP_FILE" >/tmp/openplot.pgrestore.list 2>/tmp/openplot.pgrestore.err; then
    echo "[openplot-db-init] openplot.dump invalido; usando fallback $FALLBACK_DUMP_FILE."
    run_pg_restore "$FALLBACK_DUMP_FILE"
    echo "[openplot-db-init] Restore do dump concluido."
    exit 0
  fi

  # Alguns dumps custom podem chegar "alargados" em UTF-16 (BOM + bytes intercalados com 00).
  # Nesse caso, reconverte para bytes brutos e tenta pg_restore novamente.
  if [ "$BOM_HEX" = "fffe" ] || [ "$BOM_HEX" = "feff" ]; then
    if command -v iconv >/dev/null 2>&1; then
      CANDIDATE_RAW="/tmp/openplot.dump.raw"
      if [ "$BOM_HEX" = "fffe" ]; then
        iconv -f UTF-16LE -t ISO-8859-1 "$DUMP_FILE" > "$CANDIDATE_RAW"
      else
        iconv -f UTF-16BE -t ISO-8859-1 "$DUMP_FILE" > "$CANDIDATE_RAW"
      fi

      if pg_restore --list "$CANDIDATE_RAW" >/tmp/openplot.pgrestore.list 2>/tmp/openplot.pgrestore.err; then
        echo "[openplot-db-init] Dump custom detectado apos normalizacao de UTF-16; usando pg_restore."
        RESTORE_FILE="$CANDIDATE_RAW"
      else
        CANDIDATE_RAW_STRIPPED="/tmp/openplot.dump.raw.stripped"
        dd if="$CANDIDATE_RAW" of="$CANDIDATE_RAW_STRIPPED" bs=1 skip=1 2>/dev/null
        if pg_restore --list "$CANDIDATE_RAW_STRIPPED" >/tmp/openplot.pgrestore.list 2>/tmp/openplot.pgrestore.err; then
          echo "[openplot-db-init] Dump custom detectado apos remover BOM normalizado; usando pg_restore."
          RESTORE_FILE="$CANDIDATE_RAW_STRIPPED"
        fi
      fi

      if [ "$RESTORE_FILE" != "$DUMP_FILE" ]; then
        run_pg_restore "$RESTORE_FILE"
        echo "[openplot-db-init] Restore do dump concluido."
        exit 0
      fi
    else
      echo "[openplot-db-init] iconv indisponivel para normalizar dump UTF-16."
      exit 1
    fi
  fi

  echo "[openplot-db-init] Formato detectado: SQL plano (psql)."
  SQL_FILE="$DUMP_FILE"

  if [ "$BOM_HEX" = "fffe" ] || [ "$BOM_HEX" = "feff" ]; then
    if command -v iconv >/dev/null 2>&1; then
      echo "[openplot-db-init] Encoding UTF-16 detectado; convertendo para UTF-8 antes do restore."
      SQL_FILE="/tmp/openplot.dump.utf8.sql"
      if [ "$BOM_HEX" = "fffe" ]; then
        iconv -f UTF-16LE -t UTF-8 "$DUMP_FILE" > "$SQL_FILE"
      else
        iconv -f UTF-16BE -t UTF-8 "$DUMP_FILE" > "$SQL_FILE"
      fi
    else
      echo "[openplot-db-init] iconv indisponivel para converter dump UTF-16."
      exit 1
    fi
  fi

  psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 \
    --file "$SQL_FILE"
fi

echo "[openplot-db-init] Restore do dump concluido."
