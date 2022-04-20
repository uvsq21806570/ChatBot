import pydoc
import random
import json
from colorama import Cursor
from matplotlib.pyplot import connect
import pyodbc
import torch

from model import NeuralNet
from nltk_utils import bag_of_words, tokenize
from query_db import *

server = "pollution-data.database.windows.net"
database = "AirPollutionDB"
username = "Paco"
password = "uvsq21806570Tostaky78_"
cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

with open('intents.json', 'r') as json_data:
    intents = json.load(json_data)

FILE = "data.pth"
data = torch.load(FILE)

#informations
input_size = data["input_size"]
hidden_size = data["hidden_size"]
output_size = data["output_size"]
all_words = data['all_words']
tags = data['tags']
model_state = data["model_state"]

model = NeuralNet(input_size, hidden_size, output_size).to(device)
model.load_state_dict(model_state)
model.eval()
bot_name = "ChatBot"
print("Let's chat! (type 'quit' to exit)")
while True:
    # sentence = "do you use credit cards?"

    sentence = input("You: ")
    if sentence == "quit":
        break
    #tokenize permet de decouper en mots la phrase entré par l'utilisateur.
    sentence = tokenize(sentence)
    X = bag_of_words(sentence, all_words)
    X = X.reshape(1, X.shape[0])
    X = torch.from_numpy(X).to(device)#because our bag of words function returns in numpy array

    output = model(X)
    _, predicted = torch.max(output, dim=1)

    tag = tags[predicted.item()]


    probs = torch.softmax(output, dim=1) # permet d'evaluer la probabilitéx
    prob = probs[0][predicted.item()]
    
    if prob.item() > 0.75:
        if tag == "day_ranking":
            reponse = day_ranking(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "min_pollution_hour":
            reponse = min_pollution_hour(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "max_pollution_hour":
            reponse = max_pollution_hour(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "loc_ranking":
            reponse = loc_ranking(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "recent_news":
            reponse = recent_news(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "hour_ranking":
            reponse = hour_ranking(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "max_pollution_day":
            reponse = max_pollution_day(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "min_pollution_day":
            reponse = min_pollution_day(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "wrong_place_wrong_time":
            reponse = wrong_place_wrong_time(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "wrong_place_wrong_date":
            reponse = wrong_place_wrong_date(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "versailles_current_pollution":
            reponse = versailles_current_pollution(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "lille_current_pollution":
            reponse = lille_current_pollution(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "nice_current_pollution":
            reponse = nice_current_pollution(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "brest_current_pollution":
            reponse = brest_current_pollution(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "bayonne_current_pollution":
            reponse = bayonne_current_pollution(cursor)
            print(f"{bot_name}: {reponse}")
        elif tag == "strasbourg_current_pollution":
            reponse = strasbourg_current_pollution(cursor)
            print(f"{bot_name}: {reponse}")        
        else:
            for intent in intents['intents']:
                if tag == intent["tag"]:
                    print(f"{bot_name}: {random.choice(intent['responses'])}")
    else:
        print(f"{bot_name}: I do not understand...")