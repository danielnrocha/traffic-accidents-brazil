# Acidentes de Estradas no Brasil
## Projeto para a disciplina de Banco de Dados - FGV-RJ

![image](https://user-images.githubusercontent.com/42002377/143804427-5225ff0c-31c4-428e-89e4-7d4c531d98e5.png)
![image](https://user-images.githubusercontent.com/42002377/143804508-66ccab83-5edf-463d-871c-5d39d4646b93.png)

### Metodologia:
- Coletamos dados do [Kaggle](https://www.kaggle.com/mcamera/brazil-highway-traffic-accidents), obtidos originalmente da Polícia Rodoviária Federal (PRF)
- Obtivemos o [GeoJson](https://www.kaggle.com/thiagobodruk/brazil-geojson) contendo as geometrias dos estados brasileiros (permite plotar os mapas)
- [Modelamos](modelagem.xlsx) os dados em 6 tabelas (com as respectivas relações e chaves)
- Criamos o banco no AWS RDS e populamos as tabelas
- Plotamos os gráficos usando GeoPandas e Matplotlib
