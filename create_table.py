from sqlalchemy.exc import OperationalError, DatabaseError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import mysql.connector as mysql
import pandas as pd
import logging

# Logging Setup
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()])

def connect_to_db(schema, database):
    """Query database and fetch table data.

    Args:        
        schema (str): MySQL table schema (e.g. 'heroku_aba902d59bebc6b').
        database (str): MySQL table name (e.g. 'acidente').
    """
    
    db_user = 'bd7bbf83ab7643'
    db_password = 'dfce7117'
    db_host = 'us-cdbr-east-04.cleardb.com'
    db_port = '3306'
    
    params = f'{db_user}:{db_password}@{db_host}:{db_port}/{schema}'
    
    try:
        engine = create_engine("mysql+mysqlconnector://%s" % params, max_identifier_length=128, pool_size=1)
        engine.connect()
        Session = sessionmaker(bind=engine)
        session = Session()
    except Exception:
        logging.error("%s - Could not connect to database", database)

    db_tb = pd.read_sql(f"SELECT * FROM {schema}.{database}", session.bind)

    return db_tb, engine, session


def push_to_db(schema, database, dataframe):
    """Update database table with new data from 'diff'.

    Args:
        schema (str): MySQL table schema (e.g. 'heroku_aba902d59bebc6b').
        database (str): MySQL table name (e.g. 'acidente').
        dataframe (df): dataframe to be inserted in the database (e.g. acidente)
    """

    db_tb, engine, session = connect_to_db(schema, database)
    logging.info("%s - Connect to database", database)

    # Update the difference between recently acquired data and already available data
    diff = dataframe.merge(db_tb, how='outer', indicator=True).loc[lambda x:x['_merge'] == 'left_only'].drop(['_merge'], axis=1)

    if diff.shape[0] > 0:

        try:
            diff.to_sql(schema=schema, name=database, con=engine, chunksize=1000, index=False, if_exists='append')
            logging.info("%s - Push to Database - Success - New Data: %s rows", database, len(diff.index))
            session.commit()
        except (OperationalError, DatabaseError):
            logging.error("%s - Push to Database Fail", database)
            session.rollback()
    else:

        logging.info("%s - Nothing to update", database)

    engine.dispose()
    session.close()
    

def populate_tables(acidente, localidade, pessoa, tipo_acidente, causa_acidente, veiculo):
    """Insert data in all the tables, after cleaning it.
    """
    
    push_to_db('heroku_aba902d59bebc6b', 'acidente', acidente)
    push_to_db('heroku_aba902d59bebc6b', 'localidade', localidade)
    push_to_db('heroku_aba902d59bebc6b', 'pessoa', pessoa)
    push_to_db('heroku_aba902d59bebc6b', 'tipo_acidente', tipo_acidente)
    push_to_db('heroku_aba902d59bebc6b', 'causa_acidente', causa_acidente)
    push_to_db('heroku_aba902d59bebc6b', 'veiculo', veiculo)
    
    
def create_tables():
    """Create all tables: acidente, localidade, pessoa, veiculo, causa_acidente e tipo_acidente.
    """
    
    db = mysql.connect(
        host = "us-cdbr-east-04.cleardb.com",
        user = "bd7bbf83ab7643",
        password = "dfce7117",
        database='heroku_aba902d59bebc6b')

    cursor = db.cursor()
    
    cursor.execute("""
                CREATE TABLE acidente (
                    id_acidente INT(10),
                    id_pessoa INT(10),
                    id_veiculo INT(10),
                    data DATE,
                    dia_semana VARCHAR(60),
                    horario TIME,
                    classificacao_acidente VARCHAR(60),
                    fase_dia VARCHAR(60),
                    sentido_via VARCHAR(60),
                    condicao_metereologica VARCHAR(60),
                    qtd_pessoas INT(10),
                    qtd_mortos INT(10),
                    qtd_feridos_leves INT(10),
                    qtd_feridos_graves INT(10),
                    qtd_ilesos INT(10),
                    qtd_ignorados INT(10),
                    qtd_feridos INT(10),
                    qtd_veiculos INT(10)
                    )""")

    cursor.execute("""
                CREATE TABLE localidade (
                    id_local INT(10),
                    id_acidente INT(10),
                    latitude FLOAT(10, 10),
                    longitude FLOAT(10, 10),
                    uf VARCHAR(2),
                    municipio VARCHAR(60),
                    br INT(3),
                    km FLOAT(5, 5),
                    tipo_pista VARCHAR(60),
                    tracado_via VARCHAR(60),
                    uso_solo VARCHAR(60),
                    regional VARCHAR(60),
                    delegacia VARCHAR(60),
                    uop VARCHAR(60)
                    )""")

    cursor.execute("""
                CREATE TABLE pessoa (
                    id_pessoa INT(10),
                    idade INT(10),
                    sexo VARCHAR(60),
                    nacionalidade VARCHAR(60),
                    naturalidade VARCHAR(60),
                    tipo_envolvido VARCHAR(60),
                    estado_fisico VARCHAR(60)
                    )""")

    cursor.execute("""
                CREATE TABLE veiculo (
                    id_veiculo INT(10),
                    id_pessoa INT(10),
                    tipo_veiculo VARCHAR(60),
                    marca VARCHAR(60),
                    ano_fabricacao_veiculo INT(4)
                    )""")

    cursor.execute("""
                CREATE TABLE causa_acidente (
                    id_causa INT(10),
                    id_acidente INT(10),
                    id_pessoa INT(10),
                    causa_acidente VARCHAR(60),
                    causa_principal BOOL
                    )""")

    cursor.execute("""
                CREATE TABLE tipo_acidente (
                    id_tipo INT(10),
                    id_acidente INT(10),
                    id_pessoa INT(10),
                    tipo_acidente VARCHAR(60),
                    ordem_tipo_acidente INT(10)
                    )""")
        
    
def group_data():
    """Group data from all the years available.
    """
    
    por_pessoa_todos_tipos = pd.concat(list(map(lambda x: pd.read_csv(f'Downloads\\por_pessoa_todos tipos\\acidentes{x}_todas_causas_tipos.csv',
                                                    sep=';', encoding='latin-1'),
                            range(2017, 2022))), axis=0).replace('NA', pd.NA)

    por_pessoa = pd.concat(list(map(lambda x: pd.read_csv(f'Downloads\\por_pessoa\\acidentes{x}.csv',
                                                    sep=';', encoding='latin-1'),
                            range(2007, 2022))), axis=0).replace('(null)', pd.NA).replace('NA', pd.NA)

    por_ocorrencia = pd.concat(list(map(lambda x: pd.read_csv(f'Downloads\\por_ocorrencia\\datatran{x}.csv',
                                                    sep=';', encoding='latin-1', low_memory=False),
                            range(2007, 2022))), axis=0).replace('(null)', pd.NA)

    return por_pessoa_todos_tipos, por_pessoa, por_ocorrencia


def restructure_data(por_pessoa_todos_tipos, por_pessoa, por_ocorrencia):
    """Select and rename columns and merge data.
    """
    
    por_ocorrencia = por_ocorrencia[['id', 'pessoas', 'mortos', 'feridos_leves', 'feridos_graves', 'ilesos', 'ignorados', 'feridos', 'veiculos']]
    por_ocorrencia = por_ocorrencia.rename(columns={'pessoas': 'qtd_pessoas', 'mortos': 'qtd_mortos', 'feridos_leves': 'qtd_feridos_leves',
                                'feridos_graves': 'qtd_feridos_graves', 'ilesos': 'qtd_ilesos', 'ignorados': 'qtd_ignorados',
                                'feridos': 'qtd_feridos', 'veiculos': 'qtd_veiculos'})

    por_pessoa = por_pessoa[['pesid', 'nacionalidade', 'naturalidade']]

    df = pd.merge(pd.merge(por_ocorrencia, por_pessoa_todos_tipos),
                pd.merge(por_pessoa.dropna(how='all'), por_pessoa_todos_tipos))

    df.rename(columns={'id': 'id_acidente', 'data_inversa': 'data', 'pesid': 'id_pessoa'}, inplace=True) 

    df.drop(columns=['ilesos', 'feridos_graves', 'feridos_leves', 'mortos'], inplace=True)

    df = df.loc[df['tipo_veiculo'] != 'Não Informado']
    
    return df


def format_data(df):
    """Format data types to fit database formats.
    """
    
    df.data = pd.to_datetime(df.data)
    # df.horario = pd.to_datetime(df.horario)
    
    df.km = pd.to_numeric(df.km.str.replace(',', '.'))
    df.latitude = pd.to_numeric(df.latitude.str.replace(',', '.'))
    df.longitude = pd.to_numeric(df.longitude.str.replace(',', '.'))
    df.causa_principal = df.causa_principal.replace('Sim', True).replace('Não', False).replace('NÃ£o', False)

    df.br = pd.to_numeric(df.br)
    df.idade = pd.to_numeric(df.idade)
    df.ano_fabricacao_veiculo = pd.to_numeric(df.ano_fabricacao_veiculo)
    
    df.id_acidente = pd.to_numeric(df.id_acidente, downcast='integer')
    df.id_veiculo = pd.to_numeric(df.id_veiculo, downcast='integer')
    df.id_pessoa = pd.to_numeric(df.id_pessoa, downcast='integer')
    df.ordem_tipo_acidente = pd.to_numeric(df.ordem_tipo_acidente, downcast='integer')

    return df


def split_dataframes(df):
    """Split data in different dataframes to fit the database model.
    """
    
    acidente = df[['id_acidente', 'id_pessoa', 'id_veiculo', 'data', 'dia_semana', 'horario', 'classificacao_acidente', 
                'fase_dia', 'sentido_via', 'condicao_metereologica', 'qtd_pessoas', 'qtd_mortos', 'qtd_feridos_leves', 
                'qtd_feridos_graves', 'qtd_ilesos', 'qtd_ignorados', 'qtd_feridos', 'qtd_veiculos']]

    localidade = df[['id_acidente','latitude', 'longitude', 'uf', 'municipio', 'br', 'km', 'tipo_pista', 'tracado_via', 'uso_solo', 'regional', 'delegacia', 'uop']]
    localidade = localidade.assign(id_local=range(1, len(localidade)+1))

    pessoa = df[['id_pessoa', 'idade', 'sexo', 'nacionalidade', 'naturalidade', 'tipo_envolvido', 'estado_fisico']].drop_duplicates()

    tipo_acidente = df[['id_acidente', 'id_pessoa', 'tipo_acidente', 'ordem_tipo_acidente']]
    tipo_acidente = tipo_acidente.assign(id_tipo=range(1, len(tipo_acidente)+1))

    causa_acidente = df[['id_acidente', 'id_pessoa', 'causa_acidente', 'causa_principal']]
    causa_acidente = causa_acidente.assign(id_causa=range(1, len(causa_acidente)+1))

    veiculo = df[['id_veiculo', 'id_pessoa', 'tipo_veiculo', 'marca', 'ano_fabricacao_veiculo']].drop_duplicates()

    return acidente, localidade, pessoa, tipo_acidente, causa_acidente, veiculo


# create_tables()
df = restructure_data(group_data())
df = format_data(df)
populate_tables(split_dataframes(df))

# base = pd.read_csv('Downloads\\por_pessoa_todos tipos\\acidentes2007_todas_causas_tipos.csv', sep=';', encoding='latin-1')
# base[base['id']==18][['tipo_acidente', 'ordem_tipo_acidente', 'causa_acidente', 'causa_principal', 'id', 'pesid']]
