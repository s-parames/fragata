import pymysql
import html
import numpy as np
import re

import random
import json 

from tqdm.auto import tqdm

import matplotlib.pyplot as plt


db = pymysql.connect(
    host="",
    user="",
    password="",
    database="rt4")

cursor = db.cursor()

colas = [3, 5, 30]
nomesColas = ['Sistemas', 'Aplicacions', 'BigData']

#'resolved', 'open', 'new' -> Hai tickets que aparecen como open pero que levan resoltos anos, debo de estar facendo algo mal ou é un bug do RT

#queue = 0
for queue in range(len(colas)):

    

    cursor.execute("select * from Tickets")
    tickets = cursor.fetchall()

    cursor.execute("select id from Tickets WHERE Queue=%d and Status in ('resolved')" % colas[queue])
    idsTickets = cursor.fetchall()
    

    cursor.execute("select Status from Tickets WHERE Queue=%d and Status in ('resolved')" % colas[queue])
    statusTickets = cursor.fetchall()

    cursor.execute("select LastUpdated from Tickets WHERE Queue=%d and Status in ('resolved')" % colas[queue])
    datesTickets = cursor.fetchall() 

    cursor.execute("select Subject from Tickets WHERE Queue=%d and Status in ('resolved')" % colas[queue])
    subjectTickets = cursor.fetchall() 

    cursor.execute("select * from Users")
    users = cursor.fetchall() 

    id_users = np.array([users[i][0] for i in range(len(users))])

    id_tickets = np.array([tickets[i][0] for i in range(len(tickets))])


    N = 15000

    #positions = [-764, -567, -450, -400, -397, -390, -380, -350, -322, -323, -123, -2]

    conversations = []

    for l in tqdm(np.arange(len(idsTickets))[-N:]):
        cursor.execute("select * from Transactions WHERE ObjectId=%s and ObjectType='RT::Ticket'", idsTickets[l])
        ticketTransactions = cursor.fetchall()
        
        #cursor.execute("select id from Transactions WHERE ObjectId=%s and ObjectType='RT::Ticket'", idsTickets[l])
        #ticketTransactionIds = cursor.fetchall()

        #cursor.execute("select Type from Transactions WHERE ObjectId=%s and ObjectType='RT::Ticket'", idsTickets[l])
        #ticketTransactionTypes = cursor.fetchall()

        ticketTransactionIds = [ticketTransactions[i][0] for i in range(len(ticketTransactions))]
        ticketTransactionTypes = [ticketTransactions[i][4] for i in range(len(ticketTransactions))]
        
        messages = []

        for id_i in ticketTransactionIds:
            cursor.execute("select content from Attachments WHERE TransactionId=%s", id_i)
            text = cursor.fetchall()
            if len(text) == 0:
                #print(f'{id_i} was empty')
                messages.append('')
                continue

            elif len(text) > 1: 
                for j in range(len(text)):
                    if type(text[j][0]) == type(None):
                        continue
                    else:
                        text = (text[j],)
                        break
            
                
            #print(html.unescape(text[0][0].decode()))
            try:
                messages.append(html.unescape(text[0][0].decode()))
            except AttributeError:
                #print(f'O ticket con ID {l}, ten unha mensaxe baleira na mensaxe {j}')
                messages.append('')
            except UnicodeDecodeError:
                messages.append('')
        #Agora quero sacar todos os índices onde ticketTransactions[i][4], +1 para obter o seguinte
        #Formateamos as mensaxes para que poidan ser entendidas por un LLM
        
        messages_id = np.where(np.logical_or(np.array(ticketTransactionTypes) == b'Create', np.logical_or(np.array(ticketTransactionTypes) == b'Correspond', np.array(ticketTransactionTypes) == b'Comment')))[0] + 1
        
        #comments_id = np.where(np.array(ticketTransactionTypes) == b'Comment')[0] + 1

        messages_i = []

        #comments = [messages[k] for k in comments_id]

        isTechnician = []
        transactionUsers = []

        oldMessageKeywords = ['escribiu:\n', 'escribió:\n', 'wrote:\n', 'De: ', 'From: ']
        createMessageKeywords = ['helpdesk_aplicaciones@cesga.es\n', 'helpdesk_sistemas@cesga.es\n', '* Descripcion:']
        
        
        for j in messages_id:

            if ticketTransactionTypes[j-1] == b'Comment':
                messages_i.append(messages[j-1]) # Os comentarios non están en grupos de tres, polo que hai que desfacer o + 1 do índice ó ler o texto
                isTechnician.append(2) # Se é comentario, poñemolo como 2

            else:
                id_writer = np.where(id_users==ticketTransactions[j-1][-2])[0][0] # j-1 para que colla a primeira mensaxe do bloque de tres
                transactionUsers.append(id_writer)
                isTechnician.append(1 if 'cesga' in users[id_writer][5] else 0) # Se é usuario 0 e se é resposta de técnico 1

                message_j =  messages[j] # As mensáxes gárdanse en grupos de tres, detéctanse co primeiro pero o texto está no segundo

                if j >= 3:
                    for keyword in oldMessageKeywords:
                        if keyword in message_j:
                            message_j = '\n'.join(message_j.split(keyword)[0].split('\n')[:-1])

                for createKeyword in createMessageKeywords:
                    if createKeyword in message_j:
                        message_j = '\n'.join(message_j.split(createKeyword)[-1].split('\n')[1:])

                message_j = message_j.replace((f'\n<URL: https://rt.lan.cesga.es/Ticket/Display.html?id={idsTickets[l][0]} >\n\n'), '')
                message_j = re.sub(r'(?m)^\s*[-_]{3,}\s*$', '', message_j)

                messages_i.append(message_j)

        isTechnician[0] = 0 # Forzamos a que a primeira mensaxe sempre sexa dun usuario, porque ás veces hai que introducir un correo a man e queda o técnico como autor

        #messages_formated = [{"role": "system", "content": "You are a HPC tecnician answering incidences and questions from users"}]

        messages_formated = []

        for k in range(len(messages_i)):

            role = "user" if isTechnician[k] == 0 else "assistant" if isTechnician[k] == 1 else "comment"

            messages_formated.append({"role": role, "content": messages_i[k]})

        conversations.append(messages_formated)

    

    # Conversación para RAG

    def write_jsonl(path, convos):
        with open(path, "w", encoding="utf-8") as f:
            for i in range(len(convos) ):
                for j in range(len(convos[i])):
                    convos[i][j]["content"] = re.sub(r'\n{1,6}', ' ', convos[i][j]["content"])
                #if len(convo) >= 2 and convo[0]["role"] == "user" and convo[1]["role"] == "assistant":# and len(convo[0]["content"]) + len(convo[1]["content"]) < :
                example = {"link": f"https://rt.lan.cesga.es/Ticket/Display.html?id={idsTickets[-N:][i][0]}", 
                           "lastUpdated": f"{datesTickets[-N:][i][0]}", 
                           "status":statusTickets[i][0].decode(),
                           "subject": '' if type(subjectTickets[i][0]) == type(None) else html.unescape(subjectTickets[i][0].decode('utf-8', errors = 'replace')),
                           "messages": convos[i]}
                f.write(json.dumps(example, ensure_ascii=False) + "\n")

    write_jsonl(f"Chats{nomesColas[queue]}Dates_subject_resolved_comments.jsonl", conversations)

db.close()

convos = conversations
path = f"Chats{nomesColas[queue]}Dates_subject_resolved_comments.jsonl"

with open(path, "w", encoding="utf-8") as f:
        for i in range(len(convos) ):
            for j in range(len(convos[i])):
                convos[i][j]["content"] = re.sub(r'\n{1,6}', ' ', convos[i][j]["content"])
            #if len(convo) >= 2 and convo[0]["role"] == "user" and convo[1]["role"] == "assistant":# and len(convo[0]["content"]) + len(convo[1]["content"]) < :
            example = {"lastUpdated": f"{datesTickets[-N:][i][0]}", 
                       "status":statusTickets[i][0].decode(),
                       "link": f"https://rt.lan.cesga.es/Ticket/Display.html?id={idsTickets[-N:][i][0]}", 
                       "subject": '' if type(subjectTickets[i][0]) == type(None) else html.unescape(subjectTickets[i][0].decode('utf-8', errors = 'replace')),
                       "messages": convos[i]}
            f.write(json.dumps(example, ensure_ascii=False) + "\n")