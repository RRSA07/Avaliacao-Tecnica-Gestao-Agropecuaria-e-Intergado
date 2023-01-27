import numpy as np
import pandas as pd
import os
import sys
from pandas.tseries.offsets import DateOffset
from datetime import datetime,date
import xlrd
import json

def api(ti):
    # URL para obter os dados do IPCA pela API do banco central
    url = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{cod_bcb}/dados?formato=json'.format(cod_bcb='0433')

    # Dataframe com os dados da requisição
    dfBcb = pd.read_json(url).rename(columns={'data':'Data','valor':'IPCA'})

    # Converter a coluna de data para o formato de data e para o primeiro dia do mês
    dfBcb['Data'] = pd.to_datetime(dfBcb['Data'], dayfirst=True)

    # Calcular a variação do índice
    dfBcb['variacao'] = 1+(dfBcb['IPCA'])/100

    # Cálculo para encontrar o acumulado de cada ano multiplicando a varicao mês a mês para cada ano 
    ponteiro=11
    row_new_df=0
    dfAcumulated=pd.DataFrame()
    while ponteiro <= len(dfBcb)-1:
        row_df = ponteiro
        produto = 1
        while row_df >= ponteiro-11:
            produto=(dfBcb.loc[row_df,'variacao'])*produto
            row_df = row_df-1
        dfAcumulated.loc[row_new_df,'Data']=dfBcb.loc[ponteiro,'Data']
        dfAcumulated.loc[row_new_df,'Acumulado no Ano']=(produto - 1)*100
        row_new_df = row_new_df+1
        ponteiro = ponteiro + 12

    # Para ser enviado a outra task deve-se converter o arquivo em json
    json_result = dfBcb.to_json(orient="split")
    ti.xcom_push(key='dfBcb',value=json_result)

def tratarPlanilhaCepea(ti):
    # Formatando o caminho do arquvivo
    filename_cepea = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'include','cepea-consulta-20230116155544.xls')

    # Leitura do arquivo excel formatando a coluna de data no formato data e desconsiderando as três primeiras linhas do arquivo
    dfCepea = pd.read_excel(filename_cepea,converters={'Data':pd.to_datetime},skiprows=[0,1,2])

    # Colocando as datas para o primeiro dia do mês
    dfCepea['Data'] = dfCepea['Data'].apply(lambda x: x.replace(day=1))

    # Formatando o coluna de valor substituindo o caractere de virgula por ponto para converte-lo para o tipo numerico
    dfCepea['Valor'] = dfCepea['Valor'].str.replace(',','.').astype(float)

    # Completando as informações da planilha que estão faltando, usnado o mês correspondente e o valor anterior
    for row in range(len(dfCepea)):
        if (isinstance(dfCepea.loc[row, 'Data'], type(pd.NaT))):
            dfCepea.loc[row,'Data'] = dfCepea.loc[(row-1),'Data'] + DateOffset(months=1)
        if pd.isna(dfCepea.loc[row,'Valor']):
            dfCepea.loc[row,'Valor'] = dfCepea.loc[(row-1),'Valor']

    # Para ser enviado a outra task deve-se converter o arquivo em json 
    json_result = dfCepea.to_json(orient="split")
    ti.xcom_push(key='dfCepea',value=json_result)

def tratarPlanilhaBoiGordo(ti):
    # Paramêtros vindos das tasks anteriores
    json_cepea = ti.xcom_pull(key='dfCepea',task_ids='planilha_cepea')
    json_bcb = ti.xcom_pull(key='dfBcb',task_ids='request_api')

    # Transformando em dataframe
    dfBcb = pd.read_json(json_bcb, orient='split')
    dfCepea = pd.read_json(json_cepea, orient='split')
    
    # Ajustando o tipo das datas
    dfBcb['Data'] = pd.to_datetime(dfBcb['Data'], unit='ms', dayfirst=True)
    dfCepea['Data'] = pd.to_datetime(dfCepea['Data'], unit='ms', dayfirst=True)

    # Realizando o Merge entre as planilhas de Cepea e da API com os dados do IPCA
    dfMerge = pd.merge(dfCepea,dfBcb,how='left',left_on='Data',right_on='Data')

    # Calculando a correção do valor para a data base de 12/2022
    # Ordenando o dataframme pela coluna de data para calcular a correção com base na ultima linha do dataframe que será a data mais atual
    dfMerge = dfMerge.sort_values('Data')
    for x in range(len(dfMerge)):
        produto = 1
        y=x
        while y <= len(dfMerge)-1:
            produto = dfMerge.loc[y,'variacao'] * produto
            y=y+1
        dfMerge.loc[x,'Real'] = dfMerge.loc[x,'Valor'] * produto

    # Setando como index a data para ficar mais facil localizar os valores com a data com paramêtro de localização
    dfMerge = dfMerge.set_index('Data')
    
    # Formanto caminho com a localização da planilha
    filename_csv = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'include','boi_gordo_base.csv')

    # Lendo arquivo csv
    dfBoiGordoBase = pd.read_csv(filename_csv,converters={'dt_cmdty':pd.to_datetime, 'dt_etl':pd.to_datetime})

    # Colocando as datas para o primeiro dia do mês
    dfBoiGordoBase['dt_cmdty'] = dfBoiGordoBase['dt_cmdty'].apply(lambda x: x.replace(day=1))

    # Realizando o update dos valores na planilha de acordo com a data, caso essa já exista só atualizo os campos que foram indicados
    for row in range(len(dfBoiGordoBase)):
        if str(dfBoiGordoBase.loc[row,'dt_cmdty']) in list(dfMerge.index.values):
            date_update = dfBoiGordoBase.loc[row,'dt_cmdty'].date()
            dfBoiGordoBase.loc[row,'cmdty_var_mes_perc'] = ((dfMerge.loc[date_update,'Real'] - dfBoiGordoBase.loc[row,'cmdty_vl_rs_um'])/100)
            dfBoiGordoBase.loc[row,'cmdty_vl_rs_um'] = dfMerge.loc[date_update,'Real']

    # Realizando o insert das datas e os valores que não estavam no arquivo
    for index in dfMerge.index.values:
        if index not in np.unique(dfBoiGordoBase['dt_cmdty']):
            insert = [
                index,
                'Boi_Gordo',
                'Indicador do Boi Gordo CEPEA/B3',
                '15 Kg/carcaça',
                dfMerge.loc[index,'Real'],
                ((dfMerge.loc[index,'Real'] - dfMerge.loc[index,'Valor'])/100),
                date.today()
            ]
            dfBoiGordoBase.loc[len(dfBoiGordoBase.index)] = insert

    # Ordenando a planilha pela data para melhorar a visualização dos dados
    dfBoiGordoBase = dfBoiGordoBase.sort_values('dt_cmdty')

    dfBoiGordoBase = dfBoiGordoBase.round({'cmdty_vl_rs_um': 2, 'cmdty_var_mes_perc': 2})
    dfBoiGordoBase['dt_etl'] = pd.to_datetime(dfBoiGordoBase['dt_etl'])

    # Atulizando a planilha com os inserts e updates
    dfBoiGordoBase.to_csv(filename_csv,index=False)

    # Alterando o nome das colunas para particionar no arquivo parquet
    dfBoiGordoBase.rename(columns = {'dt_cmdty':'data do commoditie do dataframe', 
                                    'nome_cmdty':'Boi_Gordo',
                                    'tipo_cmdty':'Indicador do Boi Gordo CEPEA/B3',
                                    'cmdty_um':'15 Kg/carcaça',
                                    'cmdty_vl_rs_um':'valor real do commoditie do dataframe',
                                    'cmdty_var_mes_perc':'valor do cálculo realizado de variação percentual',
                                    'dt_etl':'data atual'}, inplace = True)

    # Caminho para gravar o arquivo
    filename_parquet = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'include','boi_gordo_base.parquet')

    # Gravando em formato Parquet
    dfBoiGordoBase.to_parquet(filename_parquet,partition_cols=['data do commoditie do dataframe',
                                                                    'Boi_Gordo',
                                                                    'Indicador do Boi Gordo CEPEA/B3',
                                                                    '15 Kg/carcaça',
                                                                    'valor real do commoditie do dataframe',
                                                                    'valor do cálculo realizado de variação percentual',
                                                                    'data atual'])

def main():
    # Caso seja executado direto na linha de comando
    dfapi = api()
    dfcep = tratarPlanilhaCepea()
    tratarPlanilhaBoiGordo(dfapi,dfcep)
    
if __name__ == "__main__":
    main()