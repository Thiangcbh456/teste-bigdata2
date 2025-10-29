import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import glob

# =========================
# 0) CONFIGURAÇÃO DE CAMINHOS
# =========================

# Caminho da pasta onde estão os arquivos .parquet
PASTA_CAGED = "/content/CAGED"

# Verificar se a pasta existe
if not os.path.exists(PASTA_CAGED):
    raise FileNotFoundError(f"A pasta '{PASTA_CAGED}' não foi encontrada.")

# Buscar todos os arquivos .parquet que comecem com 'caged_'
ARQUIVOS_PARQUET_BUSCA = os.path.join(PASTA_CAGED, "caged_*.parquet")

# =========================
# 1) FUNÇÕES AUXILIARES
# =========================

def fmt_br(x, pos=None):
    try:
        return f"{int(x):,}".replace(',', '.')
    except Exception:
        return str(x)

def rotulos_pt(dt_index):
    pt_abbr = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    return [f"{pt_abbr[m-1]}/{y}" for y, m in zip(dt_index.year, dt_index.month)]

def plot_barras_lado_a_lado(idx, valores_ad, valores_des, titulo='Admissões vs Desligamentos por mês'):
    x = np.arange(len(idx))
    labels = rotulos_pt(idx)

    width = 0.4
    fig, ax = plt.subplots(figsize=(12, 6))

    ax.bar(x - width/2, valores_ad, width, label='Admissões', color='#2E7D32')
    ax.bar(x + width/2, valores_des, width, label='Desligamentos', color='#C62828')

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel('Mês de Competência', fontsize=11)
    ax.set_ylabel('Contagem de Registros', fontsize=11)
    ax.set_title(titulo, fontsize=14)
    ax.legend()
    ax.grid(True, linestyle='--', alpha=0.4, axis='y')
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_br))

    ymax = max(max(valores_ad) if len(valores_ad) else 0, max(valores_des) if len(valores_des) else 0)
    offset = ymax * 0.01 if ymax > 0 else 1
    for i, (a_val, d_val) in enumerate(zip(valores_ad, valores_des)):
        ax.text(x[i] - width/2, a_val + offset, fmt_br(a_val), ha='center', va='bottom', fontsize=9, color='#2E7D32')
        ax.text(x[i] + width/2, d_val + offset, fmt_br(d_val), ha='center', va='bottom', fontsize=9, color='#C62828')

    plt.tight_layout()
    plt.show()

# =========================
# 2) CARREGAR ARQUIVOS PARQUET
# =========================

arquivos_parquet = glob.glob(ARQUIVOS_PARQUET_BUSCA)

if not arquivos_parquet:
    raise FileNotFoundError(f"Nenhum arquivo .parquet encontrado em {PASTA_CAGED}")

print(f"✅ Arquivos encontrados: {len(arquivos_parquet)}")

# Carregar e consolidar
dfs = []
for arquivo in arquivos_parquet:
    df_temp = pd.read_parquet(arquivo)
    dfs.append(df_temp)

df = pd.concat(dfs, ignore_index=True)
print(f"✅ Total de registros carregados: {len(df):,}".replace(',', '.'))

# =========================
# 3) CHECAGENS E PREPARAÇÃO
# =========================

# Coluna de mês
if 'mes_competencia' in df.columns:
    col_mes = 'mes_competencia'
elif 'competenciamov' in df.columns:
    col_mes = 'competenciamov'
else:
    raise ValueError("Não encontrei coluna de mês ('mes_competencia' ou 'competenciamov').")

# Coluna de saldo movimentação
if 'saldomovimentacao' not in df.columns:
    raise ValueError("Não encontrei a coluna 'saldomovimentacao'.")

# Converter para datetime e numeric
df['mes'] = pd.to_datetime(df[col_mes].astype(str), format='%Y%m', errors='coerce')
df = df.dropna(subset=['mes'])
df['saldomovimentacao'] = pd.to_numeric(df['saldomovimentacao'], errors='coerce')

df_valid = df[df['saldomovimentacao'].isin([1, -1])].copy()
if df_valid.empty:
    raise SystemExit("Nenhum registro com saldomovimentacao em {1, -1} foi encontrado.")

df_valid['Admissão'] = (df_valid['saldomovimentacao'] == 1).astype(int)
df_valid['Desligamento'] = (df_valid['saldomovimentacao'] == -1).astype(int)

# Pivot para gráfico
pivot = df_valid.groupby('mes')[['Admissão', 'Desligamento']].sum().sort_index()

# =========================
# 4) PLOTAR GRÁFICO
# =========================

plot_barras_lado_a_lado(pivot.index, pivot['Admissão'].values, pivot['Desligamento'].values)

# =========================
# 5) EXIBIR TABELA AGREGADA
# =========================

print("\n--- Tabela agregada por mês ---")
print(pivot.reset_index().rename(columns={'mes': 'Mês'}))