## How to read THIS file:

This file contains a brief explanation to what every datafile contains. In bold is the name of the file, and then every line represents a column with its name followed by its Python type, it's SQL type and its description. Example:

**example.csv**

column1: int(), int(11) : description

column2: str(), varchar(60) : description

## Content

**por_ocorrencia/**

id: int(), autoincrement PK: Identifier

data_inversa: date(), date: date on the dd/mm/yyyy format

horario: time(), ???: time of the occurency

uf: str(), varchar(2): state where it happened

br: str(), varchar(3): highway where it happened. Is a 3 digit number

km: float(), ????: in which kilometer of the highway it happened

municipio: str(), varchar(60): city where it happended

causa_acidente: str(), varchar(60): cause of the accident

tipo_acidente: str(), enum: type of the accidentac

classificacao_acidente: str(), enum: classification of the accident

fase_dia: str(), enum: in which stage of the day it happened

sentido_via: str(), enum: way of the road

condicao_metereologica: str(), enum: weather condition

tipo_pista: str(), enum: type of the road

tracado_via: str(), enum: wheter it was a straight of curvy road

uso_solo: str(), enum: use of the soil

ano: int(), int(4): year of the accident

pessoas: int(), int: how many people involved in the accident

mortos: int(), int: how many of the involved people died

feridos_leves: int(), int: how many of the involved people got slightly injured

feridos_graves: int(), int: how many of the involved people got severely injured

ilesos: int(), int: how many of the involved people didn't get injuries

feridos: int(), int: how many people got injured (feridos_leves+feridos_graves)

ignorados: int(), int: how many ot the involved people were ignored (pessoas-feridos-ilesos)

veiculos: int(), int: how many vehicles involved in the accident

**por_pessoa_todos_tipos/**

id: int(), autoincrement PK: Identifier of the ocurrency

pesid: int(), autoincrement PK: identifier of the person involved

data_inversa: date(), date: date on the dd/mm/yyyy format

dia_semana: str(), enum("segunda-feira",...,"domingo"): week day when the accident occured

horario: time(), time: time of the occurency

uf: str(), varchar(2): state where it happened

br: str(), varchar(3): highway where it happened. Is a 3 digit number

km: float(), ????: in which kilometer of the highway it happened

municipio: str(), varchar(30): city where it happended

causa_principal: str(), enum("sim", "não"): ???

causa_acidente: str(), varchar(60): origin of the accident (what the driver did wrong). Example: alcohol ingestion

ordem_tipo_acidente: int(), enum(1,2): ???

tipo_acidente str(), varchar(60): type of the accident (what happened because of "causa_principal"). Example: frontal colision

classificacao_acidente: str(), enum: class of the accident. The class is determined by wheter it had victims and if so wheter they got hurt or died (the worse takes priority)

fase_dia: str(), enum: in which stage of the day it happened

sentido_via: str(), enum: way of the road

condicao_metereologica: str(), enum: weather condition

tipo_pista: str(), enum: type of the road

tracado_via: str(), enum: wheter it was a straight of curvy road

uso_solo: str(), enum: use of the soil

id_veiculo: int(), integer(): identifier of the vehicle

tipo_veiculo: str(), enum(): type of the vehicle

marca: str(), enum(): brand of the vehicle

ano_fabricacao_veiculo: int(), int(4): fabrication year of the vehicle

tipo_envolvido: str(), enum("Condutor", "Passageiro"): wheter the person involved was the conductor or a passenger

estado_fisico: str(), enum(): state of the person after the accident (unharmed, injured, deceased)

idade: int(), int: age of the person

sexo: str(), enum: sex of the person

ilesos: int(), int: how many of the involved people didn't get injuries

feridos_leves: int(), int: how many of the involved people got slightly injured

feridos_graves: int(), int: how many of the involved people got severely injured

mortos: int(), int: how many of the involved people died

latitude: float(), float: latitude of the accident

longitude: float(), float: longitude of the accident

regional: ????

delegacia: ???

uop: ???
