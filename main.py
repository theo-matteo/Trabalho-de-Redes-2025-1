import pandas as pd

def processar_por_ponto():
    df = pd.read_csv("data.csv", sep=",", decimal=",", engine="python")
    df["Throughput médio Download"] = pd.to_numeric(df["Throughput médio Download"], errors="coerce")
    df["Upload"] = pd.to_numeric(df["Upload"], errors="coerce")
    agrupado = df.groupby("Ponto de medida do servidor de medição").agg(
        media_download = ("Throughput médio Download", "mean"),
        media_upload = ("Upload", "mean"),
        quantidade = ("Throughput médio Download", "count")
    ).round(2)
    agrupado.reset_index().to_csv("dados_principais.csv", index=False)
    print("Arquivo 'dados_principais.csv' criado com sucesso!")

def estatisticas_gerais():
    df = pd.read_csv("data.csv", sep=",", decimal=",", engine="python")
    colunas = {
        "Throughput médio Download": "download",
        "Upload": "upload",
        "Latencia (RTT)": "latencia",
        "Jitter": "jitter"
    }
    for col in colunas.keys():
        df[col] = pd.to_numeric(df[col], errors="coerce")
    stats_geral = df[list(colunas.keys())].agg(['mean', 'min', 'max']).rename(index={
        'mean': 'Média',
        'min': 'Mínimo',
        'max': 'Máximo'
    })
    stats_geral.columns = [colunas[col] for col in stats_geral.columns]
    stats_geral.round(2).to_csv("gerais.csv")
    print("Arquivo 'gerais.csv' criado com sucesso!")
    df["Ferramenta de Medição"] = df["Ferramenta de Medição"].fillna("Indefinido")
    
    def calc_stats(subdf):
        return subdf.agg(['mean', 'min', 'max']).rename(index={
            'mean': 'Média',
            'min': 'Mínimo',
            'max': 'Máximo'
        })

    grupos = []
    for nome, grupo in df.groupby("Ferramenta de Medição"):
        stats = calc_stats(grupo[list(colunas.keys())])
        stats.columns = [colunas[col] for col in stats.columns]
        stats['Tipo Ferramenta'] = nome
        grupos.append(stats)

    resultado = pd.concat(grupos, axis=0)
    resultado = resultado.reset_index().rename(columns={'index': 'Estatística'})
    cols = ['Tipo Ferramenta', 'Estatística'] + [c for c in resultado.columns if c not in ['Tipo Ferramenta', 'Estatística']]
    resultado = resultado[cols]

    df_pivot = resultado.pivot(index="Tipo Ferramenta", columns="Estatística")


    df_pivot.columns = [f"{estat.lower()}_{metr.lower()}" for metr, estat in df_pivot.columns]
    df_pivot = df_pivot.reset_index()
    df_pivot.round(2).to_csv("dados_por_ferramenta.csv", index=False)

    print("Arquivo 'dados_por_ferramenta.csv' criado com sucesso!")

# Executa as funções
processar_por_ponto()
estatisticas_gerais()
