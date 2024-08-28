from sqlalchemy import create_engine, text
from src.extract import extract
from src.load import load
from src.config import get_csv_to_table_mapping, DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL

# Crear una conexión a la base de datos SQLite en memoria
engine = create_engine("sqlite://")

# Extraer los datos de los archivos CSV y la API de días festivos
csv_table_mapping = get_csv_to_table_mapping()
csv_dataframes = extract(DATASET_ROOT_PATH, csv_table_mapping, PUBLIC_HOLIDAYS_URL)

# Cargar los datos en la base de datos SQLite
load(data_frames=csv_dataframes, database=engine)

# Verificar las tablas cargadas y permitir al usuario elegir una tabla para ver sus columnas
with engine.connect() as connection:
    tables = connection.execute(text("SELECT name FROM sqlite_master WHERE type='table';")).fetchall()
    print("Tablas en la base de datos:")
    for idx, table in enumerate(tables):
        print(f"{idx + 1}. {table[0]}")
    
    # Pedir al usuario que elija una tabla para ver sus columnas
    table_choice = int(input("\nSeleccione el número de la tabla que desea inspeccionar: ")) - 1
    selected_table = tables[table_choice][0]

    # Mostrar las columnas de la tabla seleccionada
    columns = connection.execute(text(f"PRAGMA table_info({selected_table});")).fetchall()
    print(f"\nColumnas en la tabla '{selected_table}':")
    for column in columns:
        print(f"{column[1]} ({column[2]})")

    # Preguntar si el usuario desea ver los datos de la tabla seleccionada
    view_data = input(f"\n¿Desea ver los datos de la tabla '{selected_table}'? (s/n): ").lower()
    if view_data == 's':
        data = connection.execute(text(f"SELECT * FROM {selected_table} LIMIT 10;")).fetchall()
        print(f"\nDatos en la tabla '{selected_table}':")
        for row in data:
            print(row)
