import requests
import pandas as pd
import psycopg2
import psycopg2.extras #serve para poder chamar colunas pelo nome
from sqlalchemy import create_engine

#Extração
# api-endpoint
URL = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/IfDataCadastro(AnoMes=@AnoMes)"

# configurando parametros obrigatórios da api
PARAMS = {
    '@AnoMes': 202209, #YYYYMM
    '$format': 'json'
}

# chamando função GET e gravando resultado em um objeto
r = requests.get(url = URL, params = PARAMS)

# analisa de o método GET obteve sucesso
# em caso de erro retorna o código do erro
if r.status_code == 200:
    print(r.text)
else:
    print(f"Error: {r.status_code}")
    r.raise_for_status()

#Tranformação
# Transforma a resposta da API de text em dicionario, padrão json
data = r.json()
# Normalizar o json semi estruturado
# Extraindo apenas o dicionario da chave 'value'
multiple_level_data = pd.json_normalize(data, record_path =['value'])

# Montando um dataframe a partir do JSON
df = pd.DataFrame.from_dict(multiple_level_data)

# Limpeza e tratamento de colunas
df2 = pd.DataFrame(df, columns= ['CodInst', 'Data', 'NomeInstituicao', 'Atividade', 'CodConglomeradoFinanceiro', 'CodConglomeradoPrudencial', 'Situacao'])

df2['Data'] = pd.to_datetime(df2['Data'], format='%Y%m')

# Ajuste no nome das colunas para o script levar ao banco de dados
df2 = df2.rename(columns={'CodInst': 'codinst', 'Data': 'anomes', 'NomeInstituicao': 'nomeinstituicao', 'Atividade': 'atividade', 'CodConglomeradoFinanceiro': 'codconglomeradofinanceiro',
                          'CodConglomeradoPrudencial': 'codconglomeradoprudencial', 'Situacao': 'situacao'})

#Armazenamento
# Para levar informações ao postgreSQL - local

hostname = 'localhost'
database = 'dev_lucas'
username = 'postgres'
pwd = 'admin'
port_id = 5432
conn = None
cur = None

engine = create_engine('postgresql+psycopg2://' + username + ':' + pwd + '@' + hostname + '/' + database)

# Criar tabela se ela não existir no db
try:
    conn = psycopg2.connect(
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id
        )

    cur = conn.cursor()

    create_script = ''' CREATE TABLE IF NOT EXISTS ifdata_codint (
                            codinst                             VARCHAR NOT NULL,
                            anomes                              date NOT NULL,
                            nomeinstituicao                     VARCHAR,
                            atividade                           VARCHAR,
                            codconglomeradofinanceiro           VARCHAR,
                            codconglomeradoprudencial           VARCHAR,
                            situacao                            VARCHAR NOT NULL) '''
    cur.execute(create_script)
    cur.close()

    conn.commit()

except Exception as error:
    print(error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

# Inserir os dados dentro da tabela criada
df2.to_sql('ifdata_codint', engine, if_exists='append', index=False)