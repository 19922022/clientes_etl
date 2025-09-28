# clientes_etl — Pipeline ETL en Python (CSV → SQLite)

## Descripción:
Este proyecto implementa un pipeline **ETL (Extract, Transform, Load)** sencillo en Python:

1. **Extracción (E):** lee un archivo CSV (`datos_clientes_raw.csv`).
2. **Transformación (T):** limpia valores nulos, convierte fechas, calcula la edad y filtra clientes mayores de 18 años.
3. **Carga (L):** guarda los datos procesados en una base SQLite (`clientes_limpios.db`) dentro de la tabla `clientes_maestros`.


---

