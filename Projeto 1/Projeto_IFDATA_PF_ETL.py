import requests
import pandas as pd
import psycopg2
import psycopg2.extras #serve para poder chamar colunas pelo nome
from sqlalchemy import create_engine

#Extração
# api-endpoint
URL = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"

# configurando parametros obrigatórios da api
PARAMS = {
    '@AnoMes': 202209, #YYYYMM
    '@TipoInstituicao': 2, #Tipo de Instituição: 1 - Conglomerados Prudenciais e Instituições Independentes, 2 - Conglomerados Financeiros e Instituições Independentes, 3 - Instituições Individuais, 4 - Instituições com Operações de Câmbio
    '@Relatorio': "\'11\'", # Carteira de crédito ativa Pessoa Física - modalidade e vencimento
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
df2 = pd.DataFrame(df, columns= ['CodInst', 'AnoMes', 'Grupo', 'NomeColuna', 'Saldo'])

df2['NomeColuna'] = df2['NomeColuna'].str.upper()
df2['AnoMes'] = pd.to_datetime(df2['AnoMes'], format='%Y%m')
df2['Saldo'] = df2['Saldo'].round(2)

# Limpeza linhas excedentes
df2 = df2[df2["NomeColuna"].str.contains("TOTAL") == False]
# Ajuste no nome das colunas para o script levar ao banco de dados
df2 = df2.rename(columns={'CodInst': 'codinst', 'AnoMes': 'anomes', 'Grupo': 'grupo', 'NomeColuna': 'nomecoluna', 'Saldo': 'saldo'})

#Armazenamento
# Para armazenamento em csv - local
df2.to_csv('dados/IFDATA_PF_db.csv', sep = ';')

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

    create_script = ''' CREATE TABLE IF NOT EXISTS ifdata_pf (
                            CodInst         VARCHAR NOT NULL,
                            AnoMes          date NOT NULL,
                            Grupo           VARCHAR NOT NULL,
                            NomeColuna      VARCHAR NOT NULL,
                            Saldo           float  ) '''
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
df2.to_sql('ifdata_pf', engine, if_exists='append', index=False)