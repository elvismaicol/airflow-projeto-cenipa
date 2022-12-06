from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

import os
from urllib.request import urlretrieve as retrieve
import pandas as pd

# constants
MY_NAME = "Elvis Maicol"
VAR_URL_CENIPA = Variable.get("url_cenipa")
VAR_RAW_PATH = Variable.get("raw_path")
VAR_STAGE_PATH = Variable.get("stage_path")
VAR_CONSUMER_PATH = Variable.get("consumer_path")
VAR_FILE_OC = Variable.get("file_oc")
VAR_FILE_OCTP = Variable.get("file_ocTipo")
VAR_FILE_AE = Variable.get("file_ae")
VAR_FILE_FAT = Variable.get("file_fat")
VAR_FILE_REC = Variable.get("file_rec")


def file_downloads(url, raw_path):
    print("Inicio do processamento dos downloads")
    # Criando uma lista com os nomes dos arquivos para download
    lista_download = ['ocorrencia.csv', 'ocorrencia_tipo.csv', 'aeronave.csv', 'recomendacao.csv',
                      'fator_contribuinte.csv']


    # loop para percorrer a "lista lista_download" e concatenando com a URL e efetuando o download dos arquivos
    cont = 0
    for f in lista_download:
        retrieve(url + lista_download[cont], raw_path + lista_download[cont])
        cont += 1
    
    # São 5 arquivos no total
    print("Quantidade : {}".format(cont))
    print("Lista de arquivos no diretório: {}".format(os.listdir(raw_path)))
    print("Fim do processamento dos downloads")

def ocorrencia_to_parquet(raw_path, stage_path, nm_file):

    print("Inicio do processamento do ocorrencias.csv")
    df_oc = pd.read_csv(raw_path + nm_file, sep=';', encoding='iso-8859-1', parse_dates=['ocorrencia_dia'])

    #Selecionando colunas para replace()
    occorrenciaCol = [
        'codigo_ocorrencia', 
        'ocorrencia_classificacao', 
        'ocorrencia_cidade',
        'ocorrencia_uf',
        'ocorrencia_pais',
        'ocorrencia_aerodromo',
        'ocorrencia_dia',
        'ocorrencia_hora',
        'investigacao_aeronave_liberada',
        'investigacao_status',
        'divulgacao_relatorio_publicado',
        'divulgacao_dia_publicacao',
        'total_recomendacoes',
        'total_aeronaves_envolvidas',
        'ocorrencia_saida_pista'
    ]

    # Criando relação de caracteres a serem substituídos
    caracterAjustes = {
        '###!':None
        , '####':None
        , '*':None
        , '**':None
        , '***':None
        , '****':None
        , '*****':None
        , 'NULL':None
        , 'null':None
    }

    # loop para percorrer as colunas e efetuar o replace
    cont = 0
    for i in occorrenciaCol:
        df_oc.replace({occorrenciaCol[cont]:caracterAjustes}, inplace=True)
        cont += 1

    # Criando colunas ano, mês e dia
    df_oc = df_oc.assign(
        ocorrencia_dia_ano = df_oc.ocorrencia_dia.dt.year, 
        ocorrencia_dia_mes = df_oc.ocorrencia_dia.dt.month, 
        ocorrencia_dia_dia = df_oc.ocorrencia_dia.dt.day
    )

    # Selecionando as colunas para escrita
    df_write = df_oc[occorrenciaCol]
    print(df_write.head())

    # Escrevendo o dataframe em parquet com compressão snappy
    df_write.to_parquet(stage_path + 'ocorrencia_parquet', compression='snappy')
    print("Fim do processamento do ocorrencias.csv")


def ocorrenciaTipo_to_parquet(raw_path, stage_path, nm_file):
    print("Inicio do processamento do ocorrenciasTipo.csv")
    df_ocTipo = pd.read_csv(raw_path + nm_file, sep=';', encoding='UTF-8')
    print(df_ocTipo.head())

    # Escrevendo o dataframe em em parquet com compressão snappy
    df_ocTipo.to_parquet(stage_path + 'ocorrenciaTipo_parquet', compression='snappy')
    print("Fim do processamento do ocorrenciasTipo.csv")

def aeronave_to_parquet(raw_path, stage_path, nm_file):
    print("Inicio do processamento do aeronave.csv")    
    df_aer = pd.read_csv(raw_path + nm_file
    , sep=';', encoding='UTF-8'
    , parse_dates=['aeronave_ano_fabricacao']
    )

    #Selecionando colunas para replace()
    aeronaveCol = [
        'codigo_ocorrencia2',
        'aeronave_matricula',
        'aeronave_operador_categoria',
        'aeronave_tipo_veiculo',
        'aeronave_fabricante',
        'aeronave_modelo',
        'aeronave_tipo_icao',
        'aeronave_motor_tipo',
        'aeronave_motor_quantidade',
        'aeronave_pmd',
        'aeronave_pmd_categoria',
        'aeronave_assentos',
        #'aeronave_ano_fabricacao', # Alterar separadamente
        'aeronave_pais_fabricante',
        'aeronave_pais_registro',
        'aeronave_registro_categoria',
        'aeronave_registro_segmento',
        'aeronave_voo_origem',
        'aeronave_voo_destino',
        'aeronave_fase_operacao',
        'aeronave_tipo_operacao',
        'aeronave_nivel_dano',
        'aeronave_fatalidades_total'
    ]

    # Criando relação de caracters a serem substituídos
    caracterAjustes = {
        '###!':None
        , '####':None
        , '*':None
        , '**':None
        , '***':None
        , '****':None
        , '*****':None
        , 'NULL':None
        , 'null':None
    }
    numAnoAjustes = {
    '0':None
    , '9999':None
    }
    # Unindo os dois dicionários de Ajustes de caracters para replace() na coluna 'aeronave_ano_fabricacao'
    numCaracter = dict(caracterAjustes, **numAnoAjustes)

    # Loop para efetuar o Replace nas colunas
    cont = 0
    for i in aeronaveCol:
        df_aer.replace({aeronaveCol[cont]:caracterAjustes}, inplace=True)
        cont += 1

    # Replace especial para a coluna 'aeronave_ano_fabricacao'   
    df_aer.replace({'aeronave_ano_fabricacao':numCaracter}, inplace=True)
    # Exibição
    print(df_aer.head())
    print(df_aer.count())

    # Escrevendo o dataframe em em parquet com compressão snappy
    df_aer.to_parquet(stage_path + 'aeronave_parquet', compression='snappy')
    print("Fim do processamento do aeronave.csv")


def fatorCont_to_parquet(raw_path, stage_path, nm_file):    
    print("Inicio do processamento do aeronave.csv")   
    df_fcon = pd.read_csv(raw_path + nm_file, sep=';', encoding='UTF-8')

    # Criando dicionário com a relação de caracters a serem substituídos
    caracterAjustes = {
        '###!':None
        , '####':None
        , '*':None
        , '**':None
        , '***':None
        , '****':None
        , '*****':None
        , 'NULL':None
        , 'null':None
    }

    # Loop para efetuar o Replace nas colunas
    cont = 0
    for i in df_fcon.columns:
        df_fcon.replace({df_fcon.columns[cont]:caracterAjustes}, inplace=True)
        cont += 1
   
    # Exibição
    print(df_fcon.head())
    print(df_fcon.count())

    # Escrevendo o dataframe em em parquet com compressão snappy
    df_fcon.to_parquet(stage_path + 'fatorCont_parquet', compression='snappy')
    print("Fim do processamento do aeronave.csv")

def recomendacao_to_parquet(raw_path, stage_path, nm_file):
    print("Inicio do processamento do recomendacao.csv")   
    df_rec = pd.read_csv(
        raw_path + nm_file, sep=';', 
        encoding='UTF-8', 
        parse_dates=[
            'recomendacao_dia_assinatura', 'recomendacao_dia_encaminhamento', 'recomendacao_dia_feedback'
            ]
    )

    # Criando dicionário com a relação de caracters a serem substituídos
    caracterAjustes = {
        '###!':None
        , '####':None
        , '*':None
        , '**':None
        , '***':None
        , '****':None
        , '*****':None
        , 'NULL':None
        , 'null':None
    }

    # Loop para efetuar o Replace nas colunas
    cont = 0
    for i in df_rec.columns:
        df_rec.replace({df_rec.columns[cont]:caracterAjustes}, inplace=True)
        cont += 1

    print(df_rec.count())
    print(df_rec.head())

    # Criando novas colunas: ano, mês e dia, a partir da coluna 'recomendacao_dia_assinatura'
    df_rec = df_rec.assign(
        recomendacao_dia_assinatura_ano = df_rec.recomendacao_dia_assinatura.dt.year, 
        recomendacao_dia_assinatura_mes = df_rec.recomendacao_dia_assinatura.dt.month, 
        recomendacao_dia_assinatura_dia = df_rec.recomendacao_dia_assinatura.dt.day
    )

    # Exibição
    print(df_rec.count())
    print(df_rec.head())

    # Escrevendo o dataframe em em parquet com compressão snappy
    df_rec.to_parquet(stage_path + 'recomendacao_parquet', compression='snappy')
    print("Fim do processamento do recomendacao.csv")


def consume_data(stage_path, consumer_path):
    nm_file = 'ocorrencia_parquet'
    df_ocorrencia = pd.read_parquet(stage_path + nm_file)
    print(df_ocorrencia.count())
    
    # Contando e agrupando as ocorrências por Estado e Cidade 
    df_agr_uf_cidade = (
        df_ocorrencia
        .groupby(by=['ocorrencia_uf','ocorrencia_cidade'], as_index=False)
        .agg(qtd_ocorrencias=('codigo_ocorrencia', 'count'))
    )

    # Agrupando quantidade de ocorrências por Estado
    df_agr_uf = (
        df_agr_uf_cidade
        .groupby(by=['ocorrencia_uf'], as_index=False)
        .agg(qtd_ocorrencias=('qtd_ocorrencias', 'sum'))
    )

    # Guardando o total de ocorrencias para calculos posteriores
    # (considerando as ocorrencias sem registro de UF ou Cidade)
    qdtTotalOc = df_ocorrencia['codigo_ocorrencia'].value_counts().sum()

    # Percentual em relação ao total por UF
    df_agr_uf['percent_total'] = ((df_agr_uf['qtd_ocorrencias'] / qdtTotalOc) * 100).round(1)

    # Escrevendo o dataframe de quantidade de ocorrências por Estado em ".orc"
    df_agr_uf.to_orc(consumer_path + 'agrupamento_uf.orc')

    # Agrupando quantidade de ocorrências por cidade
    df_agr_cidade = (
        df_ocorrencia
        .groupby(by=['ocorrencia_cidade'], as_index=False)
        .agg(qtd_ocorrencias=('codigo_ocorrencia', 'count'))
    )

    # Inserindo uma coluna som o percentual em relação oa total geral
    df_agr_cidade['percent_total'] = ((df_agr_cidade['qtd_ocorrencias'] / qdtTotalOc) * 100).round(1)

    # Escrevendo o dataframe de quantidade de ocorrências por cidade em ".orc"
    df_agr_cidade.to_orc(consumer_path + 'agrupamento_cidades.orc')

def generating_graph(consumer_path):
    from graficos import graficoBarras
    nm_file = 'agrupamento_cidades.orc'
    df_cidade = pd.read_orc(consumer_path + nm_file)

    # Selecionando os 10 Municipios com a maior quantidade de ocorrências
    df_cidade_top10 = df_cidade.sort_values(by='qtd_ocorrencias', ascending=False).head(10)

    #graficoBarras(df, var_x, var_y, nm_fig, consumer_path, title, xlabel, ylabel)
    title = 'Top 10 Cidades com Maior Qtd de Ocorrências'
    xlabel = ''
    ylabel = 'Quantidade de Ocorrências'
    nm_fig = 'grafico-barras-top10-cidade'
    graficoBarras(
        df_cidade_top10, 
        'ocorrencia_cidade', 
        'qtd_ocorrencias', 
        nm_fig, consumer_path, 
        title, xlabel, ylabel)

    # Gerando gráfico de Estados
    #graficoBarras(df, var_x, var_y, nm_fig, consumer_path, title, xlabel, ylabel)
    nm_file = 'agrupamento_uf.orc'
    df = pd.read_orc(consumer_path + nm_file) 
    df_uf = df.sort_values(by='qtd_ocorrencias', ascending=False)

    nm_fig = 'grafico-barras-agrupado-uf'    
    title = 'Quantidade de Ocorrências por Estados'
    xlabel = ''
    ylabel = ''
    graficoBarras(
        df_uf, 
        'ocorrencia_uf', 
        'qtd_ocorrencias', 
        nm_fig, consumer_path, 
        title, xlabel, ylabel)


with DAG(
    dag_id="elvis_projeto_cenipa_pipeline",
    start_date=datetime(2022, 7, 28),
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=["cenipa_pipeline"],
    default_args={
        "owner": MY_NAME,
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:

    task_downloads = PythonOperator(
        task_id='task_downloads',
        python_callable=file_downloads,
        op_kwargs={
            "url": VAR_URL_CENIPA,
            "raw_path": VAR_RAW_PATH
        }
    )

    task_ocorrencia_to_parquet = PythonOperator(
        task_id='task_ocorrencia_to_parquet',
        python_callable=ocorrencia_to_parquet,
        op_kwargs={
            "raw_path": VAR_RAW_PATH,
            "stage_path": VAR_STAGE_PATH,
            "nm_file": VAR_FILE_OC
        }
    )
    
    task_ocorrenciaTipo_to_parquet = PythonOperator(
        task_id='task_ocorrenciaTipo_to_parquet',
        python_callable=ocorrenciaTipo_to_parquet,
        op_kwargs={
            "raw_path": VAR_RAW_PATH,
            "stage_path": VAR_STAGE_PATH,
            "nm_file": VAR_FILE_OCTP
        }
    )

    task_aeronave_to_parquet = PythonOperator(
        task_id='task_aeronave_to_parquet',
        python_callable=aeronave_to_parquet,
        op_kwargs={
            "raw_path": VAR_RAW_PATH,
            "stage_path": VAR_STAGE_PATH,
            "nm_file": VAR_FILE_AE
        }
    )

    task_fatorCont_to_parquet = PythonOperator(
        task_id='task_fatorCont_to_parquet',
        python_callable=fatorCont_to_parquet,
        op_kwargs={
            "raw_path": VAR_RAW_PATH,
            "stage_path": VAR_STAGE_PATH,
            "nm_file": VAR_FILE_FAT
        }
    )

    task_recomendacao_to_parquet = PythonOperator(
        task_id='task_recomendacao_to_parquet',
        python_callable=recomendacao_to_parquet,
        op_kwargs={
            "raw_path": VAR_RAW_PATH,
            "stage_path": VAR_STAGE_PATH,
            "nm_file": VAR_FILE_REC
        }
    )

    task_consume_data = PythonOperator(
        task_id='task_consume_data',
        python_callable=consume_data,
        op_kwargs={
            "stage_path": VAR_STAGE_PATH,
            "consumer_path": VAR_CONSUMER_PATH
        }
    )

    task_generating_graph = PythonOperator(
    task_id='task_generating_graph',
    python_callable=generating_graph,
    op_kwargs={
        "consumer_path": VAR_CONSUMER_PATH
    }
    )


task_downloads >> task_ocorrencia_to_parquet >> task_ocorrenciaTipo_to_parquet >> task_recomendacao_to_parquet >> task_consume_data
task_downloads >> task_aeronave_to_parquet >> task_fatorCont_to_parquet >> task_consume_data
task_consume_data >> task_generating_graph
