import os
import sys
import ftplib
import pandas as pd
import py7zr
import pyarrow as pa
import pyarrow.parquet as pq

# --- CONFIGURAÇÃO DE PASTAS LOCAIS ---
PASTA_RAIZ = r"/content/SCRIPTS"
PASTA_ARQUIVOS = os.path.join(PASTA_RAIZ, "BRUTOS")  # pasta onde os .7z e .txt ficarão
os.makedirs(PASTA_ARQUIVOS, exist_ok=True)

ARQUIVO_PARQUET_FINAL = os.path.join(PASTA_RAIZ, "rais_ti_brasil_2023.parquet")

# --- CONFIGURAÇÃO FTP ---
FTP_HOST = "ftp.mtps.gov.br"
FTP_USER = "anonymous"
PASTA_REMOTA = "/pdet/microdados/RAIS/2023/"
ARQUIVOS_REMOTOS = ["RAIS_VINC_PUB_NORDESTE.7z"]  # Adicione outros arquivos se quiser o Brasil inteiro

# --- COLUNAS E CBO ---
COLUNAS_NECESSARIAS = [
    'Município',     
    'CBO Ocupação 2002',  
    'Vl Remun Média (SM)', 
    'Escolaridade após 2005', 
    'Idade',
    'Tipo Vínculo',
    'Qtd Hora Contr', 
]

RENOMEAR_COLUNAS = {
    'CBO Ocupação 2002': 'CBO',
    'Vl Remun Média (SM)': 'Remuneracao Media (SM)',
    'Escolaridade após 2005': 'Grau Instrucao',
    'Qtd Hora Contr': 'Horas Contratuais',
}

CBO_FILTRO_TI = [
    '2122', '2123', '2124', '2125', '2131', '2132', '3171', '3172',
    '212105', '212305', '212310', '212315', '212320', '212325',
    '212405', '212410', '212415', '212420', '212425' 
]

CHUNK_SIZE = 25000
registros_filtrados = 0
parquet_writer = None

# --- 1. DOWNLOAD DOS ARQUIVOS .7z ---
print("📥 Verificando e baixando arquivos .7z do FTP se necessário...")
try:
    with ftplib.FTP(FTP_HOST, encoding='latin-1') as ftp:
        ftp.login(user=FTP_USER)
        ftp.cwd(PASTA_REMOTA)
        for arquivo in ARQUIVOS_REMOTOS:
            caminho_local = os.path.join(PASTA_ARQUIVOS, arquivo)
            if not os.path.exists(caminho_local):
                print(f"Baixando {arquivo}...")
                with open(caminho_local, "wb") as f:
                    ftp.retrbinary(f"RETR {arquivo}", f.write)
                print(f"✅ Download concluído: {caminho_local}")
            else:
                print(f"✅ Arquivo já existe localmente: {caminho_local}")
except Exception as e:
    print(f"❌ Erro ao conectar ou baixar do FTP: {e}")
    sys.exit()

# --- 2. EXTRAÇÃO DOS .7z PARA .TXT ---
print("\n🗂️ Extraindo arquivos .7z...")
arquivos_7z = [f for f in os.listdir(PASTA_ARQUIVOS) if f.endswith(".7z")]
for arquivo_7z in arquivos_7z:
    caminho_7z = os.path.join(PASTA_ARQUIVOS, arquivo_7z)
    arquivos_no_diretorio = os.listdir(PASTA_ARQUIVOS)
    nome_txt = arquivo_7z.replace(".7z", ".txt")
    caminho_txt = os.path.join(PASTA_ARQUIVOS, nome_txt)
    
    if not os.path.exists(caminho_txt):
        print(f"Extraindo {arquivo_7z}...")
        with py7zr.SevenZipFile(caminho_7z, mode='r') as archive:
            archive.extractall(path=PASTA_ARQUIVOS)
        print(f"✅ Extraído: {caminho_txt}")
    else:
        print(f"✅ TXT já existe: {caminho_txt}")

# --- 3. PROCESSAMENTO E ETL PARA PARQUET ---
print("\n⚙️ Processando arquivos TXT e gerando Parquet filtrado...")
arquivos_txt = [f for f in os.listdir(PASTA_ARQUIVOS) if f.endswith(".txt")]
if not arquivos_txt:
    print(f"❌ Nenhum arquivo .txt encontrado na pasta: {PASTA_ARQUIVOS}")
    sys.exit()

try:
    for arquivo_txt in arquivos_txt:
        caminho_txt = os.path.join(PASTA_ARQUIVOS, arquivo_txt)
        print(f"\nProcessando {arquivo_txt}...")
        reader = pd.read_csv(
            caminho_txt,
            sep=';',
            encoding='latin-1',
            low_memory=False,
            usecols=COLUNAS_NECESSARIAS,
            chunksize=CHUNK_SIZE,
            on_bad_lines='warn'
        )

        for i, chunk in enumerate(reader):
            chunk.rename(columns=RENOMEAR_COLUNAS, inplace=True)
            chunk['CBO'] = chunk['CBO'].astype(str)

            df_ti = chunk[
                chunk['CBO'].str[:4].isin([c[:4] for c in CBO_FILTRO_TI]) |
                (chunk['CBO'].str.len().isin([4, 6]) & chunk['CBO'].isin(CBO_FILTRO_TI))
            ].copy()

            if df_ti.empty:
                continue

            table = pa.Table.from_pandas(df_ti, preserve_index=False)
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(ARQUIVO_PARQUET_FINAL, table.schema)
            parquet_writer.write_table(table)

            registros_filtrados += len(df_ti)
            print(f"   -> Bloco {i+1} processado. Total de TI Brasil: {registros_filtrados:,}", end='\r')

    if parquet_writer is not None:
        parquet_writer.close()

    print(f"\n\n✅ ETL concluído! Total de {registros_filtrados:,} registros de TI em todo o Brasil salvos em:")
    print(f"   Arquivo Parquet: {ARQUIVO_PARQUET_FINAL}")
    print("📂 Arquivos .7z e .txt foram mantidos na pasta de origem.")

except Exception as e:
    if parquet_writer is not None:
        parquet_writer.close()
    print(f"\n❌ ERRO FATAL DURANTE O PROCESSAMENTO: {e}")
    sys.exit()
