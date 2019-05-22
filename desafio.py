
#Para utilizar a API do Google Sheets, é necessário seguir os passos 1 e 2 do seguinte link:
#https://developers.google.com/sheets/api/quickstart/python

#bibliotecas para pegar a planilha
from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd   #para utilizar o Pandas
import re   #para utilizar a função "re.sub"
from datetime import datetime   #para utilizar a função "datetime.strptime" 
import xml.etree.ElementTree as et   #para importar o arquivo xml
import sqlite3  #para utilizar o sqlite
from sqlalchemy import create_engine   #para inserir os dados na tabela do bd


#Se estiver modificando esses escopos, exclua o arquivo token.pickle
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

SAMPLE_SPREADSHEET_ID = '1N6JFMIQR71HF5u5zkWthqbgpA8WYz_0ufDGadeJnhlo'   #ID da tabela
SAMPLE_RANGE_NAME = ['usuarios','dependentes']   #Nome das duas páginas da tabela


def get_google_sheet(spreadsheet_id, range_name):   #Pega a tabela e joga em um dataframe
    creds = None
    #Checagem do arquivo token.pickle, que armazena os tokens de acesso e de atualização do usuário, e é 
    #criado automaticamente quando o fluxo de autorização é concluído pela primeira vez.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    #Se não houver credenciais (válidas) disponíveis, é solicitado o login do usuário
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        #Salva as credenciais para uma tentativa futura
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()   #Chamada da API
    
    df = {}   #Criação do dict
    
    for name in range_name:   #passa pela lista de páginas da tabela
        #utiliza o id da tabela e a página atual, para pegar dados de cada página
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=name).execute()
        

        header = result.get('values', [])[0]    #Assume-se que a primeira linha é o cabeçalho
        values = result.get('values', [])[1:]   #E que o resto são os dados

        if not values:
            print('No data found.')
        else:
            all_data = []
            for col_id, col_name in enumerate(header):
                column_data = []
                for row in values:
                    try:   #tenta adicionar o valor
                        column_data.append(row[col_id])
                    except:   #se der erro, adiciona None
                        column_data.append(None)
                ds = pd.Series(data=column_data, name=col_name)
                all_data.append(ds)
            df[name] = pd.concat(all_data, axis=1)   #dict da folha atual recebe o dataframe
            
    return df

#chamada da função, atribuindo os dataframes criados à variável df
df = get_google_sheet(SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)




#importação de csv
columns = ["id", "nome", "email", "telefone", "valor", "desconto"]   #definição do nome das colunas
tabela_csv = pd.read_csv('dataApr-1-2019.csv', delimiter=';', names=columns, header=0)




def importa_xml(arquivo_xml, df_colunas):   #importação de xml, recebe o arquivo xml e o nome das colunas
        
    xtree = et.parse(arquivo_xml)
    xroot = xtree.getroot()   #pega os dados do arquivo
    tabela_xml = pd.DataFrame(columns = df_colunas)   #gera o dataframe com as colunas nomeadas
    
    for node in xroot:   #adiciona os dados no dataframe
        res = []
        #res.append(node.attrib.get(df_colunas[0]))   Não utilizado para esse arquivo
        for el in df_colunas[0:]: 
            if node is not None and node.find(el) is not None:
                res.append(node.find(el).text)
            else: 
                res.append(None)
        tabela_xml = tabela_xml.append(pd.Series(res, index = df_colunas), ignore_index = True)
        
    return tabela_xml

tabela_xml = importa_xml('dataApr-1-2019 2.xml', ["user_id", "name", "email_user", "phone", "buy_value"])
tabela_xml['desconto'] = 0   #cria a coluna de desconto, colocando o valor 0 em todas as linnhas 

#troca do nome das colunas
tabela_xml.rename(columns = {'user_id':'id', 'name':'nome', 'email_user':'email', 'phone':'telefone', 
                            'buy_value':'valor'}, inplace = True)




#concatenação dos 3 arquivos 
tabela_usuarios_final = pd.concat([df['usuarios'], tabela_csv, tabela_xml],  sort=False)




def corrige_telefone(telefone):   #O telefone deve ser +55DDDNUMERO. Ex: (+5516981773421)  
    
    if telefone is None or telefone == "":   #Se não conter telefone
        return None   #retorna um valor nulo
    telefone = re.sub('[^0-9]',"", telefone)   #filtra os elementos que o telefone pode ter, números de 0 a 9 

    try:
        telefone.index('55', 0, 2)   #procura por 55 nas primeiras posições
        #se o 55 é realmente o código do país, e não o DDD 
        return ('+'+telefone)  if len(telefone) == 12 else ('+55'+telefone)
    except:
        return ('+55'+telefone)   #se não conter 55 nas primeiras posições concatena um +55 no início
    
#executa a função em todos os telefones
tabela_usuarios_final['telefone'] = list(map(corrige_telefone, tabela_usuarios_final['telefone'].tolist()))  




def corrige_valor(valor):   #O Valor deve ser formatado como dinheiro (real). Ex: 999,00 
    
    if valor is None or valor == "":   #Se não conter valor
        return None   #retorna um valor nulo
    
    #troca as virgulas por pontos,para poder manipular como float 
    valor = float(re.sub('[^0-9.,]',"", str(valor)).replace(',', '.')) 
    #delimita em duas casas decimais, completando com zero, e troca os pontos por vírgulas
    return f'{valor:.2f}'.replace('.', ',') 

#executa a função em todos os valores
tabela_usuarios_final['valor'] = list(map(corrige_valor, tabela_usuarios_final['valor'].tolist()))




def corrige_desconto(desconto):   #corrige a coluna 'desconto' 

    if desconto is None or desconto == "-":   #Se não conter desconto, ou conter "-"
        return 0   # retorna 0

    return desconto #Caso contrário, retorna o valor do desconto 
  
#executa a função em todos os descontos 
tabela_usuarios_final['desconto'] = list(map(corrige_desconto, tabela_usuarios_final['desconto'].tolist()))




def gera_valor_com_desconto(valor, desconto):  #O valor_com_desconto deve ser calculado com o valor_total - desconto%

    valor = float(valor.replace(',', '.'))     
    valor_com_desconto = valor - (valor*float(desconto)/100) #calcula valor com desconto
    #define o padrão com duas casas decimais, arredonda os decimais, e completa com 0 quando necessário
    return ("%.2f" % round(valor_com_desconto,2)).replace('.', ',') 

#executa a função em todos os valores e descontos        
tabela_usuarios_final['valor_com_desconto'] = list(map(gera_valor_com_desconto, tabela_usuarios_final['valor'].tolist(), 
                                                tabela_usuarios_final['desconto'].tolist()))        




def converte_data_hora_timestamp(data_hora):   #Datas no formato TIMESTAMP

    if data_hora is None or data_hora == "":   #Se não conter data e hora
        return None   
    
    data_hora = datetime.strptime(data_hora,'%d/%m/%Y %H:%M:%S')  #converte os valores da tabela para datatime
    return str(datetime.timestamp(data_hora))   #converte para timestamp
    
#executa a função em todas as data/horas
df['dependentes'].data_hora = list(map(converte_data_hora_timestamp, df['dependentes'].data_hora.tolist()))




def gera_csv(tabela_usuarios_final, dependentes):   #Gera dois arquivos csv com as páginas atualizadas

    
    #Alterações na tabelas    
    #Escolhe as colunas 
    tabela_usuarios_final = tabela_usuarios_final[["id", "nome", "email", "telefone", "valor", "valor_com_desconto"]]  
    #Renomeia nomes das colunas
    tabela_usuarios_final = tabela_usuarios_final.rename(columns = {'valor':'valor_total'})
    #ordenação do dataframe pela coluna 'id'
    tabela_usuarios_final['id'] = pd.to_numeric(tabela_usuarios_final['id'])
    tabela_usuarios_final = tabela_usuarios_final.sort_values(by='id')

    #Renomeia nomes das colunas
    dependentes = dependentes.rename(columns = {'user_id':'usuario_id'})
   
    #Nomeação dos arquivos
    now = datetime.now()    #data/hora atual 
    hora_usuarios =  now.strftime("usuarios.data%b-%d-%G.csv")
    hora_dependentes =  now.strftime("dependentes.data%b-%d-%G.csv")
    
    #geração dos arquivos
    tabela_usuarios_final.to_csv(hora_usuarios, sep=';', index=False)
    dependentes.to_csv(hora_dependentes, sep=';', index=False)
    
    return {'Usuarios': tabela_usuarios_final, 'Dependentes': dependentes}   #retorna como dict
    
#Executa a função com as páginas como parâmetro
df_final = gera_csv(tabela_usuarios_final, df['dependentes'])      




def cria_banco_de_dados(db):  #Cria um banco de dados sqlite e as respectivas tabelas
    
    
    if(os.path.isfile(db)):   #verifica se o banco já existia
        os.remove(db)   #exclui banco anterior para colocar os dados novos
    conn = sqlite3.connect(db)   #conecta no arquivo
    c = conn.cursor()   #cursor pra poder fazer as operações
    
    # Criação das tabelas
    c.execute('''CREATE TABLE Usuarios(
                    id INTEGER PRIMARY KEY,
                    nome TEXT NOT NULL,
                    email TEXT,
                    telefone TEXT,
                    valor_total TEXT,
                    valor_com_desconto TEXT)''')
    c.execute('''CREATE TABLE Dependentes(
                    id INTEGER PRIMARY KEY,
                    usuario_id INTEGER NOT NULL,
                    dependente_id INTEGER NOT NULL,
                    data_hora INTEGER,
                    FOREIGN KEY(usuario_id) REFERENCES Usuarios(id),
                    FOREIGN KEY(dependente_id) REFERENCES Usuarios(id))''')
    
    conn.commit() #salva alterações, guarda o que foi feito
    conn.close() #fecha conexão


db = "importacao.db"
cria_banco_de_dados(db)



def insere_no_banco(db, data):   #Insere as informações dos dataframes no banco
    
    
    engine = create_engine(f'sqlite:///{db}', echo=False)   #conexão com o banco
    for table in list(data.keys()):   #insere os dados nas tabelas
        data[table].to_sql(table, con=engine, if_exists='append', index=False)

insere_no_banco(db, df_final)    




def consulta_no_banco(db, query):   #Realiza consultas no banco
    
    if(os.path.isfile(db)):
        conn = sqlite3.connect(db)   #conexão com o banco
        df = pd.read_sql_query(query,conn)   #executa o comando da query
        conn.close()   #fecha conexão
        return df
    return None
        
print(consulta_no_banco(db, "SELECT u1.id AS 'ID do usuário', u1.nome AS 'Usuário', u2.id AS 'ID do dependente', u2.nome AS 'Dependentes' "+                    "FROM Usuarios AS u1, Usuarios AS u2, Dependentes AS d WHERE u1.id=d.usuario_id AND u2.id=d.dependente_id"))

