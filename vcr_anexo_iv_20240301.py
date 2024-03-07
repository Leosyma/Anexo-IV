# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 10:19:48 2024

@author: 2018459
"""

#%% Bibliotecas
import pandas as pd
import numpy as np
import keyring
import cx_Oracle
import os
import glob
import math
import datetime as dt
from datetime import datetime
import re
import unidecode

#%% Leitura do arquivo
arquivos_tales_especifica = glob.glob(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\06. Base de dados Iniciais\Bases\2023\Tales\Bases Regras Específicas\*')
arquivos_tales_estruturada = glob.glob(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\06. Base de dados Iniciais\Bases\2023\Tales\Bases Estruturadas\*')
arquivos_indger = glob.glob(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\06. Base de dados Iniciais\Bases\INDGER\*')

# Arquivo Tales
# df_tales = pd.read_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\06. Base de dados Iniciais\Bases\2023\Tales\Bases Regras Específicas\Base Oficial - Padrões Específicos 12_2023.xlsx',sheet_name='Base',dtype='str')
# Bases Regras Específicas
df_tales_especifica = pd.DataFrame()
for arquivo in arquivos_tales_especifica:
    try:
        df = pd.read_excel(arquivo,sheet_name='Base',dtype='str',usecols='O:S,BF,BM,BN,BV')
        df_tales_especifica = pd.concat([df_tales_especifica,df])
    except Exception as err:
        print('ERRO!!', err)
 
# Bases Estruturadas
df_tales_estruturada = pd.DataFrame()
for arquivo in arquivos_tales_estruturada:
    try:
        df = pd.read_excel(arquivo,sheet_name='Base',dtype='str',usecols='I:R,BO,BX,BY')
        df_tales_estruturada = pd.concat([df_tales_estruturada,df])
    except Exception as err:
        print('ERRO!!', err)

# Arquivo INDGER
# df_indger = pd.read_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\SERV_RCC_PauloVictorAmorim_12_2023 V2.xlsx',sheet_name='SERVICOS',thousands=',')
df_indger = pd.DataFrame()
for arquivo in arquivos_indger:
    try:
        df = pd.read_excel(arquivo,sheet_name='SERVICOS',thousands=',')
        df_indger = pd.concat([df_indger,df])
    except Exception as err:
        print('ERRO!!', err)

# Tratamento dos dados
df_especifica = df_tales_especifica.astype('str')
df_estruturada = df_tales_estruturada.astype('str')
# df = df.replace('nan',None)
df_especifica = df_especifica.replace('nan',np.nan)
df_estruturada = df_estruturada.replace('nan',np.nan)

# Converte a chave para string
for coluna in ['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005']:
    df_indger[coluna] = df_indger[coluna].astype('str')
        
# Coloca zero a esquerda
df_especifica[['SERV_003']] = df_especifica[['SERV_003']].apply(lambda x: x.str.zfill(2))
df_estruturada[['SERV_003']] = df_estruturada[['SERV_003']].apply(lambda x: x.str.zfill(2))
df_indger[['SERV_003']] = df_indger[['SERV_003']].apply(lambda x: x.str.zfill(2))


################### INDGER x TALES ##########################
def agrupa_df_especifica(df_especifica,df_indger):    
    # Filtra somente as notas encerradas no mês
    df_encerrado_mes = df_especifica[df_especifica['ENCERRADA_NO_MES'] == 'S']
    df_encerrado_mes = df_encerrado_mes.groupby(by=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005',],as_index=False).count()
    df_encerrado_mes = df_encerrado_mes[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','ENCERRADA_NO_MES']]
    df_encerrado_mes['SERV_001'] = df_encerrado_mes['SERV_001'].replace('02016440000162','396').replace('33050196000188','63').replace('04172213000105','2937').replace('53859112000169','69')
    
    # Filtra somente as notas abertas no mês
    df_iniciada_mes = df_especifica[df_especifica['INICIADA_NO_MES'] == 'S']
    df_iniciada_mes = df_iniciada_mes.groupby(by=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005',],as_index=False).count()
    df_iniciada_mes = df_iniciada_mes[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','INICIADA_NO_MES']]
    df_iniciada_mes['SERV_001'] = df_iniciada_mes['SERV_001'].replace('02016440000162','396').replace('33050196000188','63').replace('04172213000105','2937').replace('53859112000169','69')
    
    # Filtra somente as notas com descumprimento de prazo
    df_especifica[['DESCUMPRIMENTO_PRAZO']] = df_especifica[['DESCUMPRIMENTO_PRAZO']].apply(lambda x: x.str.upper())
    df_descumpriu_prazo = df_especifica
    df_descumpriu_prazo = df_descumpriu_prazo.replace('FORA DO PRAZO','S').replace('DENTRO DO PRAZO','N')    
    df_descumpriu_prazo = df_descumpriu_prazo[(df_descumpriu_prazo['DESCUMPRIMENTO_PRAZO'] == 'S') & (df_descumpriu_prazo['ENCERRADA_NO_MES'] == 'S')]
    df_descumpriu_prazo = df_descumpriu_prazo.groupby(by=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005',],as_index=False).count()
    df_descumpriu_prazo = df_descumpriu_prazo[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','DESCUMPRIMENTO_PRAZO']]
    df_descumpriu_prazo['SERV_001'] = df_descumpriu_prazo['SERV_001'].replace('02016440000162','396').replace('33050196000188','63').replace('04172213000105','2937').replace('53859112000169','69')
    
    # Filtra somente as notas com compensação
    df_especifica['VLR_COMPENSACAO'] = df_especifica['VLR_COMPENSACAO'].astype('float')
    df_compensacao = df_especifica.groupby(by=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005',],as_index=False).sum()
    df_compensacao = df_compensacao[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','VLR_COMPENSACAO']]
    df_compensacao['SERV_001'] = df_compensacao['SERV_001'].replace('02016440000162','396').replace('33050196000188','63').replace('04172213000105','2937').replace('53859112000169','69')
    
    
    # Cruza o arquivo do Tales com INDGER
    df_merge_encerrado_mes = df_encerrado_mes.merge(df_indger,how='left',on=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005'])
    df_merge_iniciada_mes = df_iniciada_mes.merge(df_indger,how='left',on=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005'])
    df_merge_descumpriu_prazo = df_descumpriu_prazo.merge(df_indger,how='left',on=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005'])
    df_merge_compensacao = df_compensacao.merge(df_indger,how='left',on=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005'])
    
    # Seleciona somente as colunas de interesse
    df_merge_encerrado_mes = df_merge_encerrado_mes[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','ENCERRADA_NO_MES', 'SERV_006']]
    df_merge_iniciada_mes = df_merge_iniciada_mes[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','INICIADA_NO_MES','SERV_010']]
    df_merge_descumpriu_prazo = df_merge_descumpriu_prazo[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','DESCUMPRIMENTO_PRAZO','SERV_008']]
    df_merge_compensacao = df_merge_compensacao[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','VLR_COMPENSACAO','SERV_015']]
    
    
    # Exporta os arquivos
    # df_merge_encerrado_mes.to_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\07. Análises\02 - Dados\Regras Específicas\base_encerrada_mes.xlsx',index=False)
    # df_merge_iniciada_mes.to_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\07. Análises\02 - Dados\Regras Específicas\base_iniciada_mes.xlsx',index=False)
    df_merge_descumpriu_prazo.to_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\07. Análises\02 - Dados\Regras Específicas\base_descumpriu_prazo.xlsx',index=False)
    # df_merge_compensacao.to_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\07. Análises\02 - Dados\Regras Específicas\base_compensacao.xlsx',index=False)




def agrupa_df_estruturada(df_estruturada,df_indger):    
    # Filtra somente as notas encerradas no mês
    df_encerrado_mes = df_estruturada[df_estruturada['Nota do Mês'] == '1']
    df_encerrado_mes = df_encerrado_mes.groupby(by=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005',],as_index=False).count()
    df_encerrado_mes = df_encerrado_mes[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','Nota do Mês']]


    # Filtra somente as notas com descumprimento de prazo
    df_estruturada[['Transgredido']] = df_estruturada[['Transgredido']].apply(lambda x: x.str.upper())
    df_descumpriu_prazo = df_estruturada
    df_descumpriu_prazo = df_descumpriu_prazo.replace('FORA DO PRAZO','S').replace('DENTRO DO PRAZO','N')    
    df_descumpriu_prazo = df_descumpriu_prazo[(df_descumpriu_prazo['Transgredido'] == 'S') & (df_descumpriu_prazo['Nota do Mês'] == '1')]
    df_descumpriu_prazo = df_descumpriu_prazo.groupby(by=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005',],as_index=False).count()
    df_descumpriu_prazo = df_descumpriu_prazo[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','Transgredido']]


    
    # Filtra somente as notas com compensação
    df_estruturada['VlRess'] = df_estruturada['VlRess'].astype('float')
    df_compensacao = df_estruturada.groupby(by=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005',],as_index=False).sum()
    df_compensacao = df_compensacao[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','VlRess']]

    
    # Cruza o arquivo do Tales com INDGER
    df_merge_encerrado_mes = df_encerrado_mes.merge(df_indger,how='left',on=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005'])
    df_merge_descumpriu_prazo = df_descumpriu_prazo.merge(df_indger,how='left',on=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005'])
    df_merge_compensacao = df_compensacao.merge(df_indger,how='left',on=['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005'])
    
    # Seleciona somente as colunas de interesse
    df_merge_encerrado_mes = df_merge_encerrado_mes[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','Nota do Mês', 'SERV_006']]
    df_merge_descumpriu_prazo = df_merge_descumpriu_prazo[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','Transgredido','SERV_008']]
    df_merge_compensacao = df_merge_compensacao[['SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005','VlRess','SERV_015']]
    
    
    # Exporta os arquivos
    df_merge_encerrado_mes.to_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\07. Análises\02 - Dados\Base Estruturada\base_encerrada_mes.xlsx',index=False)
    # df_merge_descumpriu_prazo.to_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\07. Análises\02 - Dados\Base Estruturada\base_descumpriu_prazo.xlsx',index=False)
    # df_merge_compensacao.to_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\07. Análises\02 - Dados\Base Estruturada\base_compensacao.xlsx',index=False)


# Roda as funções
agrupa_df_especifica(df_especifica,df_indger)
agrupa_df_estruturada(df_estruturada,df_indger)




################### DE-PARA ##########################
# Arquivo do DE-PARA para as tipologias
df_de_para = pd.read_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\06. Base de dados Iniciais\Bases\2023\Tales\DE-PARA\Notas Anexo III e Anexo IV - Tabela_Oficial.xlsx',sheet_name='Base',dtype='str',usecols='A:C,F:G,H,K')

# Arquivo Tales Bases Estruturadas para comparar o DE-PARA
df_depara_estruturada = pd.DataFrame()
for arquivo in arquivos_tales_estruturada:
    try:
        df = pd.read_excel(arquivo,sheet_name='Base',dtype='str',usecols='B,I:M,AA,AG,BV')
        df_depara_estruturada = pd.concat([df_depara_estruturada,df])
    except Exception as err:
        print('ERRO!!', err)


# df_teste = pd.read_excel(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\06. Base de dados Iniciais\Bases\2023\Tales\Bases Estruturadas\Base Oficial - Padrões Estruturados 12_2023.xlsx',sheet_name='Base',dtype='str',usecols='A:B,I:M,AA,AG,BV')
# df_merge_teste = df_teste.merge(df_de_para,how='left',left_on=['Concat1'],right_on=['Concat com Notif. Code'])


df_de_para = df_de_para.drop_duplicates()
df_de_para = df_de_para.dropna()
df_merge_tipologia = df_depara_estruturada.merge(df_de_para,how='left',left_on=['Concat1'],right_on=['Concat com Notif. Code'])
df_merge_tipologia = df_merge_tipologia[['Concat1', 'SERV_001', 'SERV_002', 'SERV_003', 'SERV_004', 'SERV_005', 'Nota', 'Descr.', 'Descrição do Padrão',
                                         'Código', 'Prazo', 'DC/DU/HU', 'Descrição ','Descrição da Atividade/nota']]


# Exporta o arquivo
df_merge_tipologia.to_csv(r'X:\Conformidade\2. VCR_ARR_PA_RISK\1. VCR\2024\VCR 009-2024 - ANEXO IV  - PTA\07. Análises\02 - Dados\Base Estruturada\base_tipologia.csv',index=False)





        







