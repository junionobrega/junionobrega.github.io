"IMPORTANDO BIBLIOTECAS"
    !pip install pandera
    !pip install pyspark
    !pip install gcsfs
    !pip install mysql-connector-python

    import pandas as pd
    import pandera as pa
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pyspark.sql.types import *
    from google.cloud import storage
    import os
    import numpy as np
    import gcsfs
    from pymongo import MongoClient
    from google.cloud import storage
    from pyspark.sql.window import Window
    from datetime import datetime
    import mysql.connector
    from mysql.connector import Error


"EXTRAÇÃO DAS BASES DE DADOS"
    "DF1"
    df1 = pd.read_csv('https://storage.googleapis.com/bucket-projeto01/d.sda.pda.005.cat.202210-orig.csv',sep=';',encoding= "ISO-8859-1")
    #Analise Geral do DataFrame
    df1
    #Verificação dos tipos de colunas 
    df1.dtypes

    "DF2"
    df2 = pd.read_csv('https://storage.googleapis.com/bucket-projeto01/d.sda.pda.005.cat.202204-orig.csv',sep=';',encoding= "ISO-8859-1")
    #Analise Geral do DataFrame
    df2
    #Verificação dos tipos de colunas 
    df2.dtypes

    "DF3"
    df3 = pd.read_csv('https://storage.googleapis.com/bucket-projeto01/d.sda.pda.005.cat.202210-orig.csv',sep=';',encoding= "ISO-8859-1")
    #Analise Geral do DataFrame
    df3
    #Verificação dos tipos de colunas 
    df3.dtypes

"PRÉ-ANÁLISE"
    "DF1"
    df1.head(5)
    df1.tail(5)
    df1.head()
    df1.dtypes
    #Verificação se os dados são unicos
    df1.Sexo.is_unique

    "DF2"
    df2.head(5)
    df2.tail(5)
    df2.head()
    df2.dtypes

    "DF3"
    df3.head(5)
    df3.tail(5)
    df3.head()
    df3.dtypes

"Tratamento INICIAL de dados para subir para o Mongo DB"
    "DF1"
    #backup do dataframe original
    df1back = df1.copy()

    #Dropagem das colunas que tinham dados repetidos ou inconsistentes
    df1.drop(['Data Acidente','Data Acidente.1'],axis=1,inplace=True)

    df1.replace(['{ñ class}','0000/00','00.000.000.000.000','Zerado','{ñ','000000-Não Informado','0'],np.NaN,inplace=True)

    df1

    "DF2"
    #backup do dataframe original
    df2back = df2.copy()

    #Dropagem das colunas que tinham dados repetidos ou inconsistentes
    df2.drop(['Data Acidente','Data Acidente.1'],axis=1,inplace=True)

    df2.replace(['ñ class'],np.NaN,inplace=True)
    df2.replace(['0000/00','00.000.000.000.000','000000-Não Informado'],np.NaN,inplace=True)

    # tratando chaves e parenteses em dados, pois estes dão erro no arquivo na hora de tranformar os dados em dicionário
    df2.replace(['{','}','\(', '\)'],'',regex=True,inplace=True)

    df2

    "DF3"
    #backup do dataframe original
    df3back = df3.copy()

    #Dropagem das colunas que tinham dados repetidos ou inconsistentes
    df3.drop(['Data Acidente','Data Acidente.1'],axis=1,inplace=True)

    df3.replace(['{ñ class}','Zerado','{ñ'],np.NaN,inplace=True)
    df3.replace(['0000/00','00.000.000.000.000','000000-Não Informado'],np.NaN,inplace=True)

    # tratando chaves e parenteses em dados, pois estes dão erro no arquivo na hora de tranformar os dados em dicionário
    df3.replace(['{','}','\(', '\)'],'',regex=True,inplace=True)

    df3

"Conector do Mondo Atlas"
    "DF1"
    uri = "mongodb+srv://cluster0.euo5god.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
    client = MongoClient(uri,tls=True,tlsCertificateKeyFile='/content/X509-cert-8594082533584929987.pem')

    db1 = client['Projeto01']
    colecao = db1['original1']

    #verificar conexão/Qtd . documentos da coleçao
    doc_count = colecao.count_documents({})
    print(doc_count)

    colecao.original1.insert_one

    "DF2"
    db2 = client['Projeto01']
    colecao2 = db2['original2']

    #verificar conexão/Qtd . documentos da coleçao
    doc2_count = colecao2.count_documents({})
    print(doc2_count)

    colecao2.original2.insert_one

    "DF3"
    db3 = client['Projeto01']
    colecao3 = db3['original3']

    #verificar conexão/Qtd . documentos da coleçao
    doc3_count = colecao3.count_documents({})
    print(doc3_count)

    colecao3.original3.insert_one

"Enviar o DataFrame ORIGINAL para o MONGO"
    "DF1"
    db1 = client['CatInssOrig1']
    colecaoCatInss1 = db1['original']
    colecaoCatInss1.count_documents({})

    #Enviar o DF para colecao selecionada no mongo
    df1_dict = df1.to_dict("records")
    colecaoCatInss1.insert_many(df1_dict)

    "DF2"
    db2 = client['CatInssOrig2']
    colecaoCatInss2 = db2['original2']
    colecaoCatInss2.count_documents({})

    #Enviar o DF para colecao selecionada no mongo
    df2_dict = df2.to_dict("records")

    colecaoCatInss2.insert_many(df2_dict)

    "DF3"
    db3 = client['CatInssOrig3']
    colecaoCatInss3 = db3['original3']
    colecaoCatInss3.count_documents({})

    #Enviar o DF para colecao selecionada no mongo
    df3_dict = df3.to_dict("records")

    colecaoCatInss3.insert_many(df3_dict)

"TRATAMENTO DETALHADO DOS DADOS"
    "DF1"
    #Renomear as colunas
    df1.rename(columns={'Agente  Causador  Acidente':'agente_causador_acidente','Data Acidente':'data1_acidente','CBO':'cbo','CID-10':'cid_10','CNAE2.0 Empregador':'cnae_empregador','CNAE2.0 Empregador.1':'atividade_cnae','Emitente CAT':'emitente_cat','Espécie do benefício':'especie_beneficio','Filiação Segurado':'filiacao_segurado','Indica Óbito Acidente':'indica_obito_acidente','Munic Empr':'municipio_empregador','Natureza da Lesão':'natureza_lesao','Origem de Cadastramento CAT':'origem_cadastramento_cat','Parte Corpo Atingida':'parte_corpo_atingida','Sexo':'sexo','Tipo do Acidente':'tipo_acidente','UF  Munic.  Acidente':'uf_acidente','UF Munic. Empregador':'uf_empregador','Data Acidente.1':'data_mes_acidente','Data Despacho Benefício':'data_despacho_beneficio','Data Acidente.2':'data_acidente','Data Nascimento':'data_nascimento','Data Emissão CAT':'data_emissao_cat','CNPJ/CEI Empregador':'cnpj_empregador'},inplace=True)

    #Renomear as colunas
    df1.rename({'ñ':'Não'})
    df1.rename({'{ñ class}':'NaN'})

"Verificação de Inconsistências"
    df1.dtypes

    pd.unique(df1['municipio_empregador'])

    "DF2"
    #Renomear as colunas
    df2.rename(columns={'Agente  Causador  Acidente':'agente_causador_acidente','Data Acidente':'data1_acidente','CBO':'cbo','CID-10':'cid_10','CNAE2.0 Empregador':'cnae_empregador','CNAE2.0 Empregador.1':'atividade_cnae','Emitente CAT':'emitente_cat','Espécie do benefício':'especie_beneficio','Filiação Segurado':'filiacao_segurado','Indica Óbito Acidente':'indica_obito_acidente','Munic Empr':'municipio_empregador','Natureza da Lesão':'natureza_lesao','Origem de Cadastramento CAT':'origem_cadastramento_cat','Parte Corpo Atingida':'parte_corpo_atingida','Sexo':'sexo','Tipo do Acidente':'tipo_acidente','UF  Munic.  Acidente':'uf_acidente','UF Munic. Empregador':'uf_empregador','Data Acidente.1':'data_mes_acidente','Data Despacho Benefício':'data_despacho_beneficio','Data Acidente.2':'data_acidente','Data Nascimento':'data_nascimento','Data Emissão CAT':'data_emissao_cat','CNPJ/CEI Empregador':'cnpj_empregador'},inplace=True)

    #converter coluna dia
    df2['indica_obito_acidente'] = df2['indica_obito_acidente'].astype(str)

    #Renomear as colunas
    df2.rename({'ñ':'Não'})
    df2.rename({'ñ class':'NaN'})

    df2.dtypes

    "DF3"
    #Renomear as colunas
    df3.rename(columns={'Agente  Causador  Acidente':'agente_causador_acidente','Data Acidente':'data1_acidente','CBO':'cbo','CID-10':'cid_10','CNAE2.0 Empregador':'cnae_empregador','CNAE2.0 Empregador.1':'atividade_cnae','Emitente CAT':'emitente_cat','Espécie do benefício':'especie_beneficio','Filiação Segurado':'filiacao_segurado','Indica Óbito Acidente':'indica_obito_acidente','Munic Empr':'municipio_empregador','Natureza da Lesão':'natureza_lesao','Origem de Cadastramento CAT':'origem_cadastramento_cat','Parte Corpo Atingida':'parte_corpo_atingida','Sexo':'sexo','Tipo do Acidente':'tipo_acidente','UF  Munic.  Acidente':'uf_acidente','UF Munic. Empregador':'uf_empregador','Data Acidente.1':'data_mes_acidente','Data Despacho Benefício':'data_despacho_beneficio','Data Acidente.2':'data_acidente','Data Nascimento':'data_nascimento','Data Emissão CAT':'data_emissao_cat','CNPJ/CEI Empregador':'cnpj_empregador'},inplace=True)

"JUNÇÃO DOS DF"
    df4 = pd.concat([df1, df2, df3])

    df4

    df4.dtypes

    df4['cnae_empregador'] = df4.cnae_empregador.astype(str)

"RENOMEAÇÃO DE DADOS NÃO INFORMADOS E/OU SEM CLASSIFICAÇÃO"
    # tratando chaves e parenteses em dados, pois estes dão erro no arquivo na hora de tranformar os dados em dicionário
    df4.replace(['{','}','\(', '\)'],'',regex=True,inplace=True)

    df4['cbo'] = df4['cbo'].str.replace('ñ class', 'Não Informado')
    df4['emitente_cat'] = df4['emitente_cat'].str.replace('ñ class', 'Não Informado')
    df4['uf_acidente'] = df4['uf_acidente'].str.replace('ñ class', 'Não Informado')
    df4['indica_obito_acidente'] = df4['indica_obito_acidente'].str.replace('ñ', 'Não Informado')
    df4['atividade_cnae'] = df4['atividade_cnae'].str.replace('ñ class', 'Não Informado')
    df4['filiacao_segurado'] = df4['filiacao_segurado'].str.replace('ñ class', 'Não Informado')
    df4['origem_cadastramento_cat'] = df4['origem_cadastramento_cat'].str.replace('ñ class', 'Não Informado')
    df4['uf_acidente'] = df4['uf_acidente'].str.replace('ñ class', 'Não Informado')

"DROP DE DUPLICADAS"
    #Verificação de Duplicadas
    df4.duplicated().sum()

    df4.drop_duplicates(inplace=True)

"PLOTAGEM"
    df4.groupby(['data_mes_acidente'],dropna=False).size().sort_values(ascending=False).head(5).plot.bar(figsize=(12,8),xlabel='PERÍODOS',ylabel='QTD OCORRENCIAS')

    df4.groupby(['indica_obito_acidente'],dropna=False).size().sort_values(ascending=False).plot.pie(figsize=(8,8),ylabel='INDICATIVO DE ÓBITOS EM ACIDENTES')

    df4.groupby(['especie_beneficio'],dropna=False).size().sort_values(ascending=False).head(5).plot.bar(figsize=(12,8),xlabel='BENEFÍCIO / PROCESSO',ylabel='QTD OCORRENCIAS')

    df4.groupby(['sexo'],dropna=False).size().sort_values(ascending=False).head(5).plot.bar(figsize=(12,8),xlabel='SEXO',ylabel='QTD DE OCORRENCIAS')

"TRATAMENTOS COM PYSPARK"
    #iniciando a sessão spark
    spark = (
        SparkSession.builder
                    .master('local')
                    .appName('projetoAquecimento')
                    .config('spark.ui.port', '4050')
                    .config("spark.jars", 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-2.1.1.jar')
                    .getOrCreate()
    )

    spark

    df4.dtypes

    #montando o esquema de colunas contendo os tipos de cada coluna/renomeando
    esquema = (
        StructType([
            StructField('agente_causador_acidente', StringType()),
            StructField('cbo', StringType()),
            StructField('cid_10', StringType()),
            StructField('cnae_empregador', StringType()),
            StructField('atividade_cnae', StringType()),
            StructField('emitente_cat', StringType()),
            StructField('especie_beneficio', StringType()),
            StructField('filiacao_segurado', StringType()),
            StructField('indica_obito_acidente', StringType()),
            StructField('municipio_enpregador', StringType()),
            StructField('natureza_lesao', StringType()),
            StructField('origem_cadastramento_cat', StringType()),
            StructField('parte_corpo_atingida', StringType()),
            StructField('sexo', StringType()),
            StructField('tipo_acidente', StringType()),
            StructField('uf_acidente', StringType()),
            StructField('uf_empregador', StringType()),
            StructField('data_despacho_beneficio', StringType()),
            StructField('data_acidente', StringType()),
            StructField('data_nascimento', StringType()),
            StructField('data_emissao_cat', StringType()),
            StructField('cnpj_empregador', StringType())
        ])
    )

    df_final = df4

    #carrefando o df
    df5 = spark.createDataFrame(df_final,schema=esquema)

    df5.printSchema()

"ENCONTRANDO INCONSISTÊNCIAS E RENOMEANDO COLUNAS COM PYSPARK"
    df5.show(truncate=False)

    #renomeando colunas
    df5 = (df5.withColumnRenamed('atividade_cnae', 'cnae_atividade').withColumnRenamed('cnpj_empregador', 'cnpj_cei_empregador')
    )

    #convertendo cnpj_empregador do formato string para float
    df5 = df5.withColumn('data_acidente', df5['data_acidente'].cast('date'))

    df5 = df5.withColumn('cnpj_cei_empregador', df5['cnpj_cei_empregador'].cast('float'))

    # convertendo data do formato string para o formato yyyy/mm/dd
    df5 = df5.withColumn('data_acidente', F.to_date(F.col('data_acidente'), 'dd/MM/yyyy'))
    df5 = df5.withColumn('data_nascimento', F.to_date(F.col('data_nascimento'), 'dd/MM/yyyy'))
    df5 = df5.withColumn('data_emissao_cat', F.to_date(F.col('data_emissao_cat'), 'dd/MM/yyyy'))

    df5.dtypes

    df5.show()

    "WINDOW FUNCTIONS"
    #criar uma partição sobre o dataframe para utilizar algum tipo de classificação
    w0 = Window.partitionBy(F.col('agente_causador_acidente')).orderBy(F.col('data_acidente').desc())

    #row_number - retorna o número da linha de acordo com a partição criada
    df6 = df5.withColumn('numero_linha', F.row_number().over(w0))

    df6.show()

    #rank
    df6 = df6.withColumn('rank', F.rank().over(w0))

    df6.show()

"AGREGANDO COLUNAS"
    #diferença de entre a data do acidente e a emissão do cat
    df7 = df5.withColumn('dif_dias', F.datediff(F.col('data_emissao_cat'), F.col('data_acidente')))

    df7.show(truncate=False)

    #agrgando as colunas municipio_enpregador e uf_empregador
    df7 = df7.withColumn('municipio_uf_empregador', F.concat(F.col('municipio_enpregador'), F.col('uf_empregador')))

    df7.show(truncate=False)

"FILTRAGENS E DADOS RELEVANTES"
    #quantidade de acidentes que acabaram em óbito
    df7.filter(F.col('indica_obito_acidente').contains('Sim')).count()

    #quantidade de acidentes por tipo de acidente
    df7.groupBy('agente_causador_acidente').count().show(truncate=False)

    #quantidade de registros por profissão
    df7.groupBy('cbo').count().show(truncate=False)

    #quantas pessoas do sexo masculino tiveram acidentes
    df7.filter(F.col('sexo').contains('Masculino')).count()

    #quantas pessoas do sexo feminino tiveram acidentes
    df7.filter(F.col('sexo').contains('Feminino')).count()

"INSIGHTS USANDO SPARKSQL"
    df7.show()

    spark.sql('show databases').show()

    spark.sql('create database projeto2').show()

    spark.sql('show databases').show()

    spark.sql('use projeto1')

    spark.sql('show tables').show()

    df7.write.saveAsTable('df_final1')

    spark.sql('show tables').show()

    spark.sql('select * from df_final1').show()

    #causa do acidente e tipo da lesão
    spark.sql('select agente_causador_acidente, natureza_lesao from df_final1').show(truncate=False)

    #acidentes que acabaram em óbito
    spark.sql('select agente_causador_acidente, indica_obito_acidente from df_final1 where indica_obito_acidente = "Sim"').show(truncate=False)

    #ocupação e causa do acidente
    spark.sql('select cbo, agente_causador_acidente from df_final1').show(truncate=False)

    #atividade, ocupação e causador do acidente
    spark.sql('select cnae_atividade, cbo, agente_causador_acidente  from df_final1 ').show(truncate=False)

    #ocupação e causa do acidente por sexo
    spark.sql('select cbo, agente_causador_acidente, sexo from df_final1').show(truncate=False)

"EXPORTAÇÃO DO DATAFRAME"
    "EXPORTANDO PYSPARK TO PANDAS"
        df8 = df7.toPandas()
        print(df8)
    
    "EXPORTAÇÃO PARA O GOOGLE CLOUD STORAGE"
    #Transformando o DF em arquivo CSV
    df8.to_csv('cat_inss_trat.csv', index = False)

    #Exportando para a BUCKET
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/content/projeto-01-ed7-d82e91c422a6.json'


    client = storage.Client()
    bucket = client.get_bucket('bucket-projeto01')
        
    bucket.blob('cat_inss_trat.csv').upload_from_string(df8.to_csv(index = False), 'text/csv')

"EXPORTAÇÃO PARA O MONGO DB"
    "Conector do Mondo Atlas"
    uri = "mongodb+srv://cluster0.euo5god.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
    client = MongoClient(uri,tls=True,tlsCertificateKeyFile='/content/X509-cert-8594082533584929987.pem')
    
    db4 = client['Projeto01']
    colecao4 = db4['tratado']
    
    #verificar conexão/Qtd . documentos da coleçao
    doc_count = colecao4.count_documents({})
    print(doc_count)
    
    "Enviar o DataFrame TRATADO para o MONGO"
    db4 = client['CatInssTrat']
    colecaoCatInss4 = db4['tratado']
    colecaoCatInss4.count_documents({})
    
    #Enviar o DF para colecao selecionada no mongo
    df8_dict = df8.to_dict("records")

    colecaoCatInss4.insert_many(df8_dict)
    
    





