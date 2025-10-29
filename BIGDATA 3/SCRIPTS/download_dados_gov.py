import ftplib
import os
import pandas as pd
import py7zr

# --- CONFIGURAÇÕES DE DADOS E CONEXÃO ---
FTP_HOST = "ftp.mtps.gov.br"
FTP_PATH = "/pdet/microdados/"
FTP_USER = "anonymous"
PASTA_BASE = "NOVO CAGED"
PASTA_ANO = "2025"

# CNAEs de TI
CNAES_TI = ['6201501', '6204000', '6209100', '6202300']  # Setores de TI

# Lista dos meses para processamento
MESES = ["202501", "202502", "202503", "202504", "202505", "202506", "202507"]

# Pasta local para armazenar arquivos brutos, txt e parquet
PASTA_LOCAL = r"/content/SCRIPTS"
os.makedirs(PASTA_LOCAL, exist_ok=True)

# Mapeamento de Colunas (Fixado)
COLUNAS_MAP = {
    'competÃªnciamov': 'competenciamov', 'regiÃ£o': 'regiao', 'municÃ­pio': 'municipio', 
    'subclasse': 'cnae_subclasse', 'saldomovimentaÃ§Ã£o': 'saldomovimentacao', 
    'cbo2002ocupaÃ§Ã£o': 'cbo2002ocupacao', 'graudeinstruÃ§Ã£o': 'graudeinstrucao', 
    'raÃ§acor': 'raca_cor', 'unidadesalÃ¡riocÃ³digo': 'unidadesalariocodigo', 
    'valorsalÃ¡riofixo': 'valorsalariofixo', 'tipomovimentaÃ§Ã£o': 'tipomovimentacao'
}

# -------------------------- FUNÇÕES DO PIPELINE --------------------------

def baixar_arquivo_caged(mes, ftp):
    """Baixa o arquivo de movimentação do mês especificado."""
    nome_arquivo = f"CAGEDMOV{mes}.7z"
    caminho_destino = os.path.join(PASTA_LOCAL, nome_arquivo)
    
    print(f"\n[DOWNLOAD] Tentando baixar {nome_arquivo}...")
    try:
        ftp.cwd(mes)
        if not os.path.exists(caminho_destino):
            with open(caminho_destino, 'wb') as local_file:
                ftp.retrbinary('RETR ' + nome_arquivo, local_file.write)
            print("  -> Download concluído.")
        else:
            print("  -> Arquivo já existe localmente, pulando download.")
        return caminho_destino
    except ftplib.all_errors as e:
        print(f"  -> ❌ Erro ao baixar {nome_arquivo}: {e}")
        return None
    finally:
        ftp.cwd('..')

def processar_e_salvar_caged(caminho_7z):
    """Descompacta, processa e salva o arquivo .txt filtrado em Parquet por mês (Brasil todo)."""
    if not caminho_7z:
        return 0

    nome_arquivo_txt = caminho_7z.replace('.7z', '.txt')
    
    try:
        if not os.path.exists(nome_arquivo_txt):
            with py7zr.SevenZipFile(caminho_7z, mode='r') as archive:
                archive.extractall(path=PASTA_LOCAL)
            print("  -> Descompactação concluída.")
        else:
            print("  -> Arquivo TXT já existe, pulando extração.")

        df_caged = pd.read_csv(nome_arquivo_txt, sep=';', encoding='latin-1', low_memory=False)
        df_caged.columns = df_caged.columns.str.strip()
        df_caged = df_caged.rename(columns=COLUNAS_MAP)
        
        # Filtra apenas os CNAEs de TI
        df_ti = df_caged[df_caged['cnae_subclasse'].astype(str).str.contains('|'.join(CNAES_TI))].copy()
        
        mes = df_ti['competenciamov'].astype(str).iloc[0] if not df_ti.empty else "desconhecido"
        df_ti['mes_competencia'] = mes
        
        registros_salvos = len(df_ti)
        print(f"  -> Registros de TI no Brasil encontrados: {registros_salvos}")
        
        if registros_salvos > 0:
            nome_parquet = f'caged_ti_brasil_{mes}.parquet'
            df_ti.to_parquet(os.path.join(PASTA_LOCAL, nome_parquet))
            print(f"  -> ✅ Dados salvos em: {nome_parquet}")
        
        return registros_salvos

    except Exception as e:
        print(f"  -> ❌ Erro ao processar o arquivo TXT: {e}")
        return 0

# -------------------------- EXECUÇÃO PRINCIPAL --------------------------

total_registros_salvos = 0
contagem_mensal = {}

try:
    with ftplib.FTP(FTP_HOST, encoding='latin-1') as ftp:
        ftp.login(user=FTP_USER)
        ftp.cwd(FTP_PATH)
        ftp.cwd(PASTA_BASE)
        ftp.cwd(PASTA_ANO)
        
        print(f"Conexão estabelecida em: {PASTA_BASE}/{PASTA_ANO}")
        
        for mes in MESES:
            print(f"\n==================== MÊS: {mes} ====================")
            caminho_7z = baixar_arquivo_caged(mes, ftp)
            registros = processar_e_salvar_caged(caminho_7z)
            total_registros_salvos += registros
            if registros > 0:
                contagem_mensal[mes] = registros
        
        print("\n=======================================================")
        print("🎉 PIPELINE CAGED CONCLUÍDO COM SUCESSO!")
        print(f"Total de registros de TI no Brasil salvos: {total_registros_salvos}.")
        print("\nContagem mensal de registros de TI (Brasil):")
        for mes, contagem in contagem_mensal.items():
            print(f"Mês {mes}: {contagem} registros")
            
except ftplib.all_errors as e:
    print(f"\n❌ ERRO FATAL DE CONEXÃO FTP: {e}")
except Exception as e:
    print(f"\n❌ Ocorreu um erro inesperado: {e}")
