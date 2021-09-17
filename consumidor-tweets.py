# importando as bibliotecas
from kafka import KafkaConsumer
import json

# configuração do kafka
brokers = ['localhost:9092']
topico = 'atividade_6_kafka'
consumer = KafkaConsumer(topico, group_id='grupo1', bootstrap_servers=brokers)

total = 0;

# consumindo os dados do produtor-tweets.py
frases = ''
for mensagem in consumer:
    texto = json.loads(mensagem.value.decode('utf-8'))
    frases = frases + texto['tweet']
    total = texto['positivo'] + texto['negativo']
    print('======================')
    print('Tweets positivos:' + str(texto['positivo']))
    print('Tweets negativos:' + str(texto['negativo']))
    print('Total tweets:' + str(total))
    