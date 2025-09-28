import pandas as pd
import datetime
import sqlite3 # Librería para la base de datos

# --- PARÁMETROS DEL PIPELINE ---
INPUT_FILE = 'datos_clientes_raw.csv'
DB_FILE = 'clientes_limpios.db'  # Archivo de la base de datos de salida
TABLE_NAME = 'clientes_maestros' # Nombre de la tabla SQL
HOY = datetime.date.today()

def extraer_datos(file_path):
    """
    Fase E (Extracción): Lee el archivo CSV de origen.
    """
    print(f"[E] Iniciando extracción de: {file_path}")
    try:
        # 1. Usamos Pandas para leer el archivo CSV
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        print(f"[ERROR] Archivo no encontrado en {file_path}")
        return None

def transformar_datos(df):
    """
    Fase T (Transformación): Aplica limpieza y cálculo de métricas.
    """
    print("[T] Iniciando fase de transformación...")
    
    # 1. TRATAMIENTO DE VALORES NULOS (NULLS)
    # Si Monto_Compra es nulo, lo rellena con cero.
    df['Monto_Compra'] = df['Monto_Compra'].fillna(0)
    # Si Email es nulo, lo rellena con el texto 'NO_PROVISTO'.
    df['Email'] = df['Email'].fillna('NO_PROVISTO')
    # Elimina la fila si Fecha_Nacimiento es nula (dato crítico).
    df = df.dropna(subset=['Fecha_Nacimiento']) 
    
    # 2. CONVERSIÓN DE TIPOS DE DATOS
    df['Fecha_Nacimiento'] = pd.to_datetime(df['Fecha_Nacimiento'])
    
    # 3. CREACIÓN DE UNA NUEVA MÉTRICA (Edad)
    def calcular_edad(fecha_nacimiento):
        """Calcula la edad actual."""
        return HOY.year - fecha_nacimiento.year - ((HOY.month, HOY.day) < (fecha_nacimiento.month, fecha_nacimiento.day))

    df['Edad'] = df['Fecha_Nacimiento'].apply(calcular_edad)
    
    # 4. VALIDACIÓN DE REGLAS DE NEGOCIO
    # Filtramos para quedarnos solo con clientes mayores o iguales a 18 años.
    df_limpio = df[df['Edad'] >= 18] 
    
    print("[T] Transformación completada.")
    return df_limpio

def cargar_datos_sqlite(df_limpio, db_file, table_name):
    """
    Fase L (Carga): Guarda los datos limpios en una tabla SQLite.
    """
    print(f"[L] Iniciando fase de carga en la base de datos: {db_file}")
    
    try:
        # Crea la conexión a la base de datos (crea el archivo .db si no existe)
        conn = sqlite3.connect(db_file)
        
        # Carga el DataFrame a la tabla SQL
        df_limpio.to_sql(table_name, conn, if_exists='replace', index=False)
        
        conn.close() 
        
        print(f"¡Pipeline ETL completado con éxito! Datos cargados en la tabla '{table_name}'.")
        print(f"Se han cargado {len(df_limpio)} registros limpios.")
        
    except Exception as e:
        print(f"[ERROR DE CARGA] No se pudo cargar en SQLite: {e}")

if __name__ == "__main__":
    
    # 1. Extracción (E)
    data_frame_raw = extraer_datos(INPUT_FILE)
    
    if data_frame_raw is not None:
        
        # 2. Transformación (T)
        data_frame_limpio = transformar_datos(data_frame_raw)
        
        # 3. Carga (L)
        cargar_datos_sqlite(data_frame_limpio, DB_FILE, TABLE_NAME)
      # --- EXPORTAR A CSV para que sea visible en GitHub ---
    csv_file = "clientes_limpios.csv"
    try:
        data_frame_limpio.to_csv(csv_file, index=False)
        print(f"[INFO] Datos exportados a CSV: {csv_file}")
    except Exception as e:
        print(f"[ERROR] No se pudo exportar CSV: {e}")   
##eof
