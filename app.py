"""
01_convertir_spotify.py
------------------------
Convierte todos los archivos Hist_Audio_XXXX.json de Spotify
a CSV individuales y genera un archivo unificado listo para analizar.

Uso:
    python 01_convertir_spotify.py

Estructura esperada de carpetas:
    /tu_carpeta/
        Hist_Audio_2018.json
        Hist_Audio_2019.json
        ...
        01_convertir_spotify.py   <-- este script
"""

import json
import pandas as pd
from pathlib import Path

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

# Carpeta donde están los JSON (por defecto: misma carpeta que el script)
CARPETA_JSON = Path("data/raw")

# Carpeta donde se guardarán los CSV
CARPETA_OUTPUT = Path("./data/csv")

# Nombre del archivo unificado
ARCHIVO_UNIFICADO = "spotify_todo.csv"

# ─── COLUMNAS ÚTILES (se descartan las de podcast y audiolibros si están vacías)
COLUMNAS_RENOMBRAR = {
    "ts": "timestamp",
    "ms_played": "ms_reproducido",
    "master_metadata_track_name": "cancion",
    "master_metadata_album_artist_name": "artista",
    "master_metadata_album_album_name": "album",
    "reason_start": "motivo_inicio",
    "reason_end": "motivo_fin",
    "shuffle": "modo_aleatorio",
    "skipped": "saltada",
    "offline": "offline",
    "platform": "plataforma",
    "episode_name": "episodio",
    "episode_show_name": "podcast",
}


# ─── FUNCIONES ────────────────────────────────────────────────────────────────

def cargar_json(path: Path) -> pd.DataFrame:
    """Lee un archivo JSON de Spotify y devuelve un DataFrame."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return pd.DataFrame(data)


def limpiar_df(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica limpieza y transformaciones básicas al DataFrame."""

    # Renombrar columnas relevantes (ignorar las que no existan)
    renombrar = {k: v for k, v in COLUMNAS_RENOMBRAR.items() if k in df.columns}
    df = df.rename(columns=renombrar)

    # Convertir timestamp a datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["fecha"]     = df["timestamp"].dt.date
        df["anio"]      = df["timestamp"].dt.year
        df["mes"]       = df["timestamp"].dt.month
        df["dia"]       = df["timestamp"].dt.day
        df["hora"]      = df["timestamp"].dt.hour
        df["dia_semana"] = df["timestamp"].dt.day_name()  # 'Monday', etc.

    # Convertir ms a minutos (más legible)
    if "ms_reproducido" in df.columns:
        df["minutos"] = (df["ms_reproducido"] / 60000).round(2)

    # Tipo de contenido: canción / podcast / desconocido
    if "cancion" in df.columns and "episodio" in df.columns:
        df["tipo"] = "cancion"
        df.loc[df["cancion"].isna() & df["episodio"].notna(), "tipo"] = "podcast"
        df.loc[df["cancion"].isna() & df["episodio"].isna(), "tipo"] = "otro"

    # Eliminar columnas de audiolibros si existen (suelen estar vacías)
    cols_a_borrar = [c for c in df.columns if "audiobook" in c.lower()]
    df = df.drop(columns=cols_a_borrar, errors="ignore")

    # Eliminar duplicados exactos (por si acaso)
    df = df.drop_duplicates()

    return df


def procesar_todos(carpeta_json: Path, carpeta_output: Path) -> pd.DataFrame:
    """
    Recorre todos los archivos Hist_Audio_*.json,
    los convierte a CSV individuales y devuelve el DataFrame unificado.
    """
    archivos = sorted(carpeta_json.glob("Hist_Audio_*.json"))

    if not archivos:
        print(f"[!] No se encontraron archivos Hist_Audio_*.json en: {carpeta_json.resolve()}")
        return pd.DataFrame()

    carpeta_output.mkdir(parents=True, exist_ok=True)
    dfs = []

    print(f"Encontrados {len(archivos)} archivos JSON\n")

    for archivo in archivos:
        print(f"  Procesando: {archivo.name} ...", end=" ")
        df = cargar_json(archivo)
        df = limpiar_df(df)

        # Guardar CSV individual
        nombre_csv = archivo.stem + ".csv"
        ruta_csv = carpeta_output / nombre_csv
        df.to_csv(ruta_csv, index=False, encoding="utf-8-sig")

        print(f"{len(df):,} filas → {nombre_csv}")
        dfs.append(df)

    # Unificar todo
    df_total = pd.concat(dfs, ignore_index=True)
    df_total = df_total.sort_values("timestamp").reset_index(drop=True)

    return df_total


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print(" Spotify JSON → CSV Converter")
    print("=" * 55)

    df = procesar_todos(CARPETA_JSON, CARPETA_OUTPUT)

    if df.empty:
        print("\nNo se procesó ningún archivo. Revisá la ruta.")
    else:
        # Guardar archivo unificado
        ruta_unificado = CARPETA_OUTPUT / ARCHIVO_UNIFICADO
        df.to_csv(ruta_unificado, index=False, encoding="utf-8-sig")

        print("\n" + "─" * 55)
        print(f"  CSV individuales guardados en: {CARPETA_OUTPUT.resolve()}")
        print(f"  Archivo unificado: {ruta_unificado.resolve()}")
        print("─" * 55)

        # Resumen rápido
        canciones = df[df["tipo"] == "cancion"] if "tipo" in df.columns else df
        print(f"\n  Total de registros:   {len(df):,}")
        print(f"  Solo canciones:       {len(canciones):,}")
        if "anio" in df.columns:
            print(f"  Período:              {df['anio'].min()} → {df['anio'].max()}")
        if "minutos" in df.columns:
            horas = canciones["minutos"].sum() / 60
            print(f"  Horas totales oídas:  {horas:,.0f} hs")
        if "artista" in df.columns:
            top1 = canciones["artista"].value_counts().idxmax()
            print(f"  Artista #1:           {top1}")
        print()