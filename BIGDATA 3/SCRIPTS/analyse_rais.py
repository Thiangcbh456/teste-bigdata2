import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# =========================
# 0) CONFIGURAÇÕES DE CAMINHO
# =========================
ARQUIVO_PARQUET_FINAL = "/content/RAIS/rais_ti_brasil_2023.parquet"

# Tradução CBO
TRADUCAO_CBO_TI = {
    '2122': 'Engenheiros de Sistemas Operacionais e Redes',
    '2123': 'Analistas de Sistemas',
    '2124': 'Desenvolvedores e Analistas de Banco de Dados',
    '2125': 'Especialistas em Segurança da Informação (Cibersegurança)',
    '2131': 'Administradores de Redes',
    '2132': 'Profissionais de Banco de Dados',
    '3171': 'Técnicos em Sistemas, Suporte e Redes',
    '3172': 'Técnicos em Desenvolvimento de Software',
    '212105': 'Analista de sistemas de automação',
    '212305': 'Administrador de Banco de Dados',
    '212310': 'Analista de desenvolvimento de sistemas',
    '212315': 'Analista de sistemas',
    '212320': 'Programador de sistemas de informação',
    '212325': 'Web designer',
    '212405': 'Analista de suporte computacional',
    '212410': 'Analista de redes e de comunicação de dados',
    '212415': 'Analista de segurança de redes',
    '212420': 'Programador de sistemas de internet',
    '212425': 'Engenheiro de teleprocessamento'
}

# Tradução de municípios (apenas p/ demo)
MUNICIPIOS_TI = {
    3550308: "São Paulo",
    3304557: "Rio de Janeiro",
    5300108: "Brasília",
    2927408: "Salvador",
    2304400: "Fortaleza",
    3106200: "Belo Horizonte",
    4106902: "Curitiba",
    4314902: "Porto Alegre",
    2611606: "Recife",
    1302603: "Manaus"
}

# Mapa UF IBGE -> Nome do Estado
UF_NOMES = {
    11: "Rondônia", 12: "Acre", 13: "Amazonas", 14: "Roraima", 15: "Pará", 16: "Amapá", 17: "Tocantins",
    21: "Maranhão", 22: "Piauí", 23: "Ceará", 24: "Rio Grande do Norte", 25: "Paraíba", 26: "Pernambuco",
    27: "Alagoas", 28: "Sergipe", 29: "Bahia",
    31: "Minas Gerais", 32: "Espírito Santo", 33: "Rio de Janeiro", 35: "São Paulo",
    41: "Paraná", 42: "Santa Catarina", 43: "Rio Grande do Sul",
    50: "Mato Grosso do Sul", 51: "Mato Grosso", 52: "Goiás", 53: "Distrito Federal"
}

# =========================
# 1) FUNÇÕES AUXILIARES
# =========================
def formatar_cbo(valor):
    if pd.isna(valor):
        return None
    s = str(valor).strip().replace('.', '').replace(',', '').replace(' ', '')
    return s[-6:] if len(s) > 6 else s.zfill(6)

def traduzir_cbo(valor):
    codigo = formatar_cbo(valor)
    if codigo is None:
        return "CBO Desconhecido"
    if codigo in TRADUCAO_CBO_TI:
        return TRADUCAO_CBO_TI[codigo]
    prefixo4 = codigo[:4]
    if prefixo4 in TRADUCAO_CBO_TI:
        return TRADUCAO_CBO_TI[prefixo4]
    return "CBO Desconhecido"

def carregar_parquet(arq):
    if os.path.exists(arq):
        try:
            df = pd.read_parquet(arq)
            print(f"✅ Arquivo Parquet carregado: {arq}")
            return df
        except Exception as e:
            print(f"⚠️ Erro ao ler Parquet existente: {e}")
    print("⚠️ Criando DataFrame de exemplo para demonstração.")
    np.random.seed(42)
    n = 500
    cbos = list(TRADUCAO_CBO_TI.keys()) + ['0000']
    df_sim = pd.DataFrame({
        'CBO': np.random.choice(cbos, size=n),
        'Remuneracao Media (SM)': np.round(np.random.normal(3.5, 1.2, size=n),2),
        'Idade': np.random.randint(18, 65, size=n),
        'Horas Contratuais': np.random.choice([20,30,40,44], size=n, p=[0.1,0.1,0.6,0.2]),
        'Município': np.random.choice(list(MUNICIPIOS_TI.keys()), size=n)
    })
    return df_sim

def plotar_barras(df_plot, x_col, y_col, titulo, horizontal=False, salvar_png=None, palette="viridis"):
    plt.figure(figsize=(12,6))
    
    if horizontal:
        ax = sns.barplot(x=y_col, y=x_col, data=df_plot, palette=palette)
        plt.xlabel("Contagem")
        plt.ylabel(x_col)
    else:
        ax = sns.barplot(x=x_col, y=y_col, data=df_plot, palette=palette)
        plt.xlabel(x_col)
        plt.ylabel("Contagem")
    
    plt.title(titulo)
    
    # Adiciona os valores no topo das barras
    for p in ax.patches:
        if horizontal:
            ax.text(p.get_width() + 0.5, p.get_y() + p.get_height()/2,
                    f'{int(p.get_width()):,}'.replace(',', '.'),
                    ha='left', va='center', fontsize=10)
        else:
            ax.text(p.get_x() + p.get_width()/2, p.get_height() + 0.5,
                    f'{int(p.get_height()):,}'.replace(',', '.'),
                    ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    
    if salvar_png:
        plt.savefig(salvar_png, dpi=150)
        print(f"✅ Gráfico salvo em {salvar_png}")
    
    plt.show()

def extrair_uf_ibge(cod_mun):
    # Extrai os dois primeiros dígitos do código do município (UF IBGE)
    if pd.isna(cod_mun):
        return None
    s = ''.join(ch for ch in str(cod_mun) if ch.isdigit())
    if len(s) < 2:
        return None
    return int(s[:2])

# =========================
# 2) EXECUÇÃO PRINCIPAL
# =========================
if __name__ == "__main__":
    df = carregar_parquet(ARQUIVO_PARQUET_FINAL)

    # Padronizar colunas
    colunas_padrao = ['CBO','Remuneracao Media (SM)','Idade','Horas Contratuais','Município']
    for col in colunas_padrao:
        if col not in df.columns:
            df[col] = np.nan
    df = df[colunas_padrao]

    # Traduzir CBO e Municípios
    df['CBO_str'] = df['CBO'].apply(formatar_cbo)
    df['Nome do Cargo'] = df['CBO'].apply(traduzir_cbo)
    df['Remuneracao Media (SM)'] = pd.to_numeric(df['Remuneracao Media (SM)'], errors='coerce')
    df['Município Nome'] = df['Município'].apply(lambda x: MUNICIPIOS_TI.get(x, str(x)))

    # Extrair UF e nome do Estado
    df['UF_IBGE'] = df['Município'].apply(extrair_uf_ibge)
    df['Estado'] = df['UF_IBGE'].map(UF_NOMES).fillna(df['UF_IBGE'].astype(str))

    sns.set(style="whitegrid")

    # --------------------
    # Número de Profissionais de TI por Faixa Etária
    # --------------------
    faixas_idade = [16, 25, 35, 45, 55, 65, 75]
    labels_idade = ['16-25', '26-35', '36-45', '46-55', '56-65', '66-75']
    df['Faixa Idade'] = pd.cut(df['Idade'], bins=faixas_idade, labels=labels_idade, right=True)
    idade_contagem = df['Faixa Idade'].value_counts().reindex(labels_idade, fill_value=0).reset_index()
    idade_contagem.columns = ['Faixa Idade','Contagem']
    plotar_barras(
        idade_contagem, 'Faixa Idade', 'Contagem',
        "Número de Profissionais de TI por Faixa Etária - 2023",
        horizontal=False, salvar_png="/content/faixa_idade.png", palette="coolwarm"
    )

    # --------------------
    # Top 10 Cargos (maior no topo)
    # --------------------
    top_cargos = df['Nome do Cargo'].value_counts().head(10).reset_index()
    top_cargos.columns = ['Nome do Cargo','Contagem']

    top_cargos = top_cargos.sort_values('Contagem', ascending=False)
    top_cargos['Nome do Cargo'] = pd.Categorical(
        top_cargos['Nome do Cargo'],
        categories=top_cargos['Nome do Cargo'],
        ordered=True
    )

    plotar_barras(
        top_cargos, 'Nome do Cargo', 'Contagem',
        "Top 10 Cargos com Mais Profissionais Registrados",
        horizontal=True, salvar_png="/content/top_cargos.png", palette="viridis"
    )

    # --------------------
    # Top 10 Estados (maior no topo)
    # --------------------
    top_estados = df['Estado'].value_counts().head(10).reset_index()
    top_estados.columns = ['Estado', 'Contagem']

    top_estados = top_estados.sort_values('Contagem', ascending=False)
    top_estados['Estado'] = pd.Categorical(
        top_estados['Estado'],
        categories=top_estados['Estado'],
        ordered=True
    )

    plotar_barras(
        top_estados, 'Estado', 'Contagem',
        "Top 10 Estados com Mais Profissionais de TI",
        horizontal=True, salvar_png="/content/top_estados.png", palette="magma"
    )