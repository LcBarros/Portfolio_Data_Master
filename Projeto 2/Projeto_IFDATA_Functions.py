import requests
import pandas as pd
import psycopg2
import psycopg2.extras #serve para poder chamar colunas pelo nome
from sqlalchemy import create_engine

def Extract(mesano, relatorio):
    # Set api-endpoint
    # Configura numero relatório
    if relatorio == "codint":
        URL = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/IfDataCadastro(AnoMes=@AnoMes)"
        
    else:
        URL = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"
        if relatorio == "pf":
            relatorio_numero = "\'11\'"
        else:
            relatorio_numero = "\'13\'"

    # configura os parametros obrigatórios da api
    if relatorio == "codint":
        PARAMS = {
        '@AnoMes': mesano, #YYYYMM
        '$format': 'json'
        }
    
    else:
        PARAMS = {
        '@AnoMes': mesano, #YYYYMM
        '@TipoInstituicao': 2, #Tipo de Instituição: 1 - Conglomerados Prudenciais e Instituições Independentes, 2 - Conglomerados Financeiros e Instituições Independentes, 3 - Instituições Individuais, 4 - Instituições com Operações de Câmbio
        '@Relatorio': relatorio_numero, # Carteira de crédito ativa Pessoa Física - modalidade e vencimento
        '$format': 'json'
        }
    
    # chamando função GET e gravando resultado em um objeto
    r = requests.get(url = URL, params = PARAMS)
    
    # analisa de o método GET obteve sucesso
    # em caso de erro retorna o código do erro
    if r.status_code == 200:
        return(r)
    else:
        return("NOK")

def Transform(relatorio, r):
    try:
        if relatorio == "codint":
            #Tranformação - codint
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
            return(df2)
        else:
            #Tranformação - pf e pj
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

            # Nova coluna para mapear tipo de carteira de crédito - para juntar os 2 relatorios na mesma tabela
            if relatorio == 'pf':
                df2['TipoCarteira'] = df.apply(lambda x: 'pf', axis=1)
            else:
                df2['TipoCarteira'] = df.apply(lambda x: 'pj', axis=1)

            # Limpeza linhas excedentes
            df2 = df2[df2["NomeColuna"].str.contains("TOTAL") == False]
            # Ajuste no nome das colunas para o script levar ao banco de dados
            df2 = df2.rename(columns={'CodInst': 'codinst', 'AnoMes': 'anomes', 'Grupo': 'grupo', 'NomeColuna': 'nomecoluna', 'Saldo': 'saldo', 'TipoCarteira': 'tipo_carteira'})
            return(df2)
    except:
        return("NOK")
    
def Load(tabela, df2):
    df2 = pd.DataFrame.from_records(df2)
    try:
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

            if tabela != "ifdata_codint":
                create_script = ''' CREATE TABLE IF NOT EXISTS  ''' + tabela + '''(
                                        CodInst         VARCHAR NOT NULL,
                                        AnoMes          date NOT NULL,
                                        Grupo           VARCHAR NOT NULL,
                                        NomeColuna      VARCHAR NOT NULL,
                                        tipo_carteira    VARCHAR,
                                        Saldo           float) '''    
            else:
                create_script = ''' CREATE TABLE IF NOT EXISTS ''' + tabela + ''' (
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
    except:
        return("NOK")
    
    else:
    # Insere os dados dentro da tabela criada

        df2.to_sql(tabela, engine, if_exists='append', index=False)
        return("OK")



