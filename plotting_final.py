from create_table import connect_to_db
import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
import numpy as np

engine, session = connect_to_db(return_df=False)

acidentes_por_estado = pd.read_sql(f"""SELECT uf, COUNT(DISTINCT id_acidente) AS qtd_acidente, YEAR(data) as ano 
                                       FROM acidente 
                                       LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                       WHERE YEAR(data) = 2020 
                                       GROUP BY uf
                                       """, session.bind)

vitimas_por_estado = pd.read_sql(f"""SELECT uf, COUNT(DISTINCT id_pessoa) AS qtd_vitimas, YEAR(data) as ano 
                                     FROM acidente 
                                     LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                     WHERE YEAR(data) = 2020 
                                     GROUP BY uf""", session.bind)

obitos_por_estado = pd.read_sql(f"""SELECT uf, SUM(qtd_mortos) as qtd_mortos, YEAR(data) as ano 
                                    FROM acidente 
                                    LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                    WHERE YEAR(data) = 2020 
                                    GROUP BY uf""", session.bind)

media_obitos_por_estado = pd.read_sql(f"""SELECT uf, AVG(qtd_mortos) as letalidade, YEAR(data) as ano 
                                          FROM acidente 
                                          LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                          WHERE YEAR(data) = 2020 
                                          GROUP BY uf""", session.bind)

media_abs_obitos_por_estado = pd.read_sql(f"""SELECT uf, AVG(qtd_mortos) as letalidade, SUM(qtd_mortos) as qtd_mortos, YEAR(data) as ano 
                                              FROM acidente 
                                              LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                              WHERE YEAR(data) = 2020 
                                              GROUP BY uf""", session.bind)

evolucao_acidentes_por_estado = pd.read_sql(f"""SELECT uf, COUNT(DISTINCT id_acidente) AS qtd_acidente, YEAR(data) as ano 
                                                FROM acidente 
                                                LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                                GROUP BY uf, ano
                                                """, session.bind)

evolucao_vitimas_por_estado = pd.read_sql(f"""SELECT uf, COUNT(DISTINCT id_pessoa) AS qtd_vitimas, YEAR(data) as ano 
                                              FROM acidente 
                                              LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                              GROUP BY uf, ano""", session.bind)

evolucao_obitos_por_estado = pd.read_sql(f"""SELECT uf, SUM(qtd_mortos) as qtd_mortos, YEAR(data) as ano 
                                             FROM acidente 
                                             LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                             GROUP BY uf, ano""", session.bind)

evolucao_media_obitos_por_estado = pd.read_sql(f"""SELECT uf, AVG(qtd_mortos) as letalidade, YEAR(data) as ano 
                                                   FROM acidente 
                                                   LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                                   GROUP BY uf, ano""", session.bind)

evolucao_media_abs_obitos_por_estado = pd.read_sql(f"""SELECT uf, AVG(qtd_mortos) as letalidade, SUM(qtd_mortos) as qtd_mortos, YEAR(data) as ano 
                                                       FROM acidente 
                                                       LEFT JOIN localidade ON acidente.id_local = localidade.id_local 
                                                       GROUP BY uf, ano""", session.bind)

br_states = gpd.read_file("br_states_geo.json")
br_mun = gpd.read_file("br_mun_geo.json")

def merge_geo_df(dataframe, geojson=br_states):
    
    return gpd.GeoDataFrame(dataframe.merge(right=geojson, left_on="uf", right_on="id", how="inner")).drop("id", axis=1)
   
geo_acidentes_por_estado = merge_geo_df(acidentes_por_estado)
geo_vitimas_por_estado = merge_geo_df(vitimas_por_estado)
geo_obitos_por_estado = merge_geo_df(obitos_por_estado)
geo_media_obitos_por_estado = merge_geo_df(media_obitos_por_estado)
geo_media_abs_obitos_por_estado = merge_geo_df(media_abs_obitos_por_estado)
geo_evolucao_acidentes_por_estado = merge_geo_df(evolucao_acidentes_por_estado)
geo_evolucao_vitimas_por_estado = merge_geo_df(evolucao_vitimas_por_estado)
geo_evolucao_obitos_por_estado = merge_geo_df(evolucao_obitos_por_estado)
geo_evolucao_media_obitos_por_estado = merge_geo_df(evolucao_media_obitos_por_estado)
geo_evolucao_media_abs_obitos_por_estado = merge_geo_df(evolucao_media_abs_obitos_por_estado)

geo_acidentes_por_mun = merge_geo_df(acidentes_por_estado, br_mun)
geo_vitimas_por_mun = merge_geo_df(vitimas_por_estado, br_mun)
geo_obitos_por_mun = merge_geo_df(obitos_por_estado, br_mun)
geo_media_obitos_por_mun = merge_geo_df(media_obitos_por_estado, br_mun)
geo_media_abs_obitos_por_mun = merge_geo_df(media_abs_obitos_por_estado, br_mun)

def plot_bubbles(geo_dataframe, column_to_size, column_to_color):
    
    centroids = geo_dataframe.copy()
    centroids.geometry = geo_dataframe.centroid
    centroids['size'] = centroids[column_to_size] / 15
    
    ax = geo_dataframe.plot(facecolor='w', edgecolor='k')

    labels, index = np.unique(geo_dataframe[column_to_color], return_inverse=True)

    centroids.plot(markersize='size', edgecolor='black', linewidth=1, c=index, ax=ax, cmap='Reds')
    
    plt.axis('off')
    plt.show()

plot_bubbles(geo_media_abs_obitos_por_estado, 'qtd_mortos', 'letalidade')


def plot_geo_df(geo_dataframe, column_to_plot):
    geo_dataframe.plot(column=column_to_plot, legend=True)
    plt.axis('off')
    plt.show()

plot_geo_df(geo_acidentes_por_estado, 'qtd_acidente')
plot_geo_df(geo_vitimas_por_estado, 'qtd_vitimas')
plot_geo_df(geo_obitos_por_estado, 'qtd_mortos')
plot_geo_df(geo_media_obitos_por_estado, 'letalidade')


def plot_evolution(geo_dataframe, column_to_plot):
    
    fig, axes = plt.subplots(figsize=(8, 8), ncols=2, nrows=2)
    for ano, x, y in zip(range(2017, 2021), [0, 0, 1, 1], [0, 1, 0, 1]):
        geo_dataframe.loc[lambda x: x['ano'] == ano].plot(ax=axes[x][y], column=column_to_plot, cmap='Reds')
        axes[x][y].axis('off')
        axes[x][y].set_title(ano)
    plt.show()       
           
plot_evolution(geo_evolucao_acidentes_por_estado, 'qtd_acidente')        
plot_evolution(geo_evolucao_vitimas_por_estado, 'qtd_vitimas')        
plot_evolution(geo_evolucao_obitos_por_estado, 'qtd_mortos')        
plot_evolution(geo_evolucao_media_obitos_por_estado, 'letalidade')        

 
import json
import plotly.express as px
f = open('br_states_geo.json')
br_states = json.load(f)
fig = px.choropleth_mapbox(acidentes_por_estado, geojson=br_states, color="qtd_acidente", locations='uf', featureidkey="id", 
                           center={"lon": -50.509509, "lat": -15.217223501657344}, mapbox_style="carto-positron", zoom=3.2)
fig.show()
