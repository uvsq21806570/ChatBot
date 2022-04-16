import json
from nltk_utils import bag_of_words, tokenize, stem

with open("src/chatbot/intents.json", "r") as f:
    intents = json.load(f)

all_words = []
tags = []
xy = []
# loop through each sentence in our intents patterns, lire champs par champ
for intent in intents["intents"]:
    tag = intent["tag"]
    # add to tag list
    tags.append(tag)
    for pattern in intent["patterns"]:
        # tokenize each word in the sentence
        w = tokenize(pattern)
        # add to our words list
        all_words.extend(w)
        # add to xy pair
        print("w : " + w)
        print("tag : " + tag)
        print()
        xy.append((w, tag))