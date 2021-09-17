# bibliotecas
from datetime import datetime
import tweepy
from json import dumps
from textblob import Blobber
from textblob.sentiments import NaiveBayesAnalyzer
tb = Blobber(analyzer=NaiveBayesAnalyzer())
import nltk
nltk.download()
from kafka import KafkaProducer

# chaves de autenticação do twitter
consumer_key = '#'
consumer_secret = '#'
access_token = '#'
access_token_secret = '#'

# configuração do kafka
broker = 'localhost:9092'
topico = 'atividade_6_kafka'
producer = KafkaProducer(bootstrap_servers=[broker],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


# configuração da API twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
tweets = api.search('league of legends')
pos_tweets = 0
neg_tweets = 0

#contagem dos tweets positivos e negativos
for tweet in tweets:
    analysis = tb(str(tweet.text))
    if analysis.sentiment.count('pos'):
        pos_tweets = pos_tweets + 1
    else:
        neg_tweets = neg_tweets + 1

# coletando os dados dos tweets
for tweet in tweets:
    frase = str(tb(tweet.text))
    data_e_hora_completa = datetime.now()
    data_string = data_e_hora_completa.strftime('%Y-%m-%d %H:%M:%S')
    dados = {"tweet": frase, "horario": data_string, "positivo": pos_tweets, "negativo": neg_tweets}
    producer.send(topico, value=dados)
