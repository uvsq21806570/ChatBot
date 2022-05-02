import pickle
import tensorflow as tf
import tflearn as tfl
from train import train_data


def load_data(datafile):
    with open(datafile, "rb") as file:
        all_words, tags, bags, tags_i = pickle.load(file)
        return all_words, tags, bags, tags_i


def create_model(datafile):

    try:
        all_words, tags, bags, tags_i = load_data(datafile)
    except:
        train_data()
        all_words, tags, bags, tags_i = load_data(datafile)

    tf.reset_default_graph()

    HIDDEN_NEURONS = int((len(all_words) + len(tags)) / 2)

    net = tfl.input_data(shape=[None, len(all_words)])
    net = tfl.fully_connected(net, HIDDEN_NEURONS)
    net = tfl.fully_connected(net, HIDDEN_NEURONS)
    net = tfl.fully_connected(net, len(tags), activation="softmax")
    net = tfl.regression(net)

    model = tfl.DNN(net)

    try:
        model.load("data/model.tflearn")
    except:
        model.fit(bags, tags_i, n_epoch=50, batch_size=4, show_metric=True)
        model.save("data/model.tflearn")

    return model, all_words, tags