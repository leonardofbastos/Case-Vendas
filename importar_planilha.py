import pandas as pd
import mysql.connector

# Configurações do banco de dados
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "leonardo",
    "database": "leonardo"
}

arquivo = r"H:\Meu Drive\ESTUDOS\Case Arzz\Candidates_Test - ori.xlsx"


def importar_planilha (arquivo):

    print("\n -------------------------------------------------------------------------------- \n ")
    print(f"📢 Iniciando processamento planilha!")
    print(f"💻 Lendo arquivo ***{arquivo}*** ")

    dados = pd.read_excel(arquivo)
    nro_regs = len(arquivo)
    print(f"💻 Total de {nro_regs} encontrados.")

    dados = dados.where(pd.notna(dados), None) # corrige nan para none - necessário no mysql

    if 'ts_order' in dados.columns:
        dados['ts_order'] = pd.to_datetime(dados['ts_order'], errors='coerce') # converte formato data ts order

    print(f"💻 Conectando com banco de dados.")
    conn = mysql.connector.connect(**DB_CONFIG) # conectando db
    cursor = conn.cursor()

    print(f"💻 Comando: DROP TABLE arz_importar_vendas.")
    query_drop = """DROP TABLE arz_importar_vendas"""
    cursor.execute(query_drop)
    
    print(f"💻 Comando: CREATE TABLE arz_importar_vendas.")
    query_create = """
    CREATE TABLE IF NOT EXISTS arz_importar_vendas (
        id_item INT AUTO_INCREMENT PRIMARY KEY,
        cd_order VARCHAR(200),
        ts_order VARCHAR(200),
        cd_customer VARCHAR(200),
        ds_store VARCHAR(200),
        ds_province VARCHAR(200),
        cd_sku VARCHAR(200),
        ds_category VARCHAR(200),
        vl_full_price DECIMAL(18,4),
        vl_price DECIMAL(18,4),
        vl_cost DECIMAL(18,4),
        qt_ordered_units INT,
        qt_returned_units INT
    )
    """
    cursor.execute(query_create)

    print(f"💻 Comando: INSERT INTO arz_importar_vendas.")
    query_insert = """
    INSERT INTO arz_importar_vendas (
        cd_order,
        ts_order,
        cd_customer,
        ds_store,
        ds_province,
        cd_sku,
        ds_category,
        vl_full_price,
        vl_price,
        vl_cost,
        qt_ordered_units,
        qt_returned_units
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for _, row in dados.iterrows():
        cursor.execute(query_insert, tuple(row.replace({pd.NA: None, float("nan"): None})))

    conn.commit()
    cursor.close()
    conn.close()

    print(f'❗ Processamento planilha finalizado!')
    print("\n -------------------------------------------------------------------------------- \n ")

importar_planilha(arquivo)
