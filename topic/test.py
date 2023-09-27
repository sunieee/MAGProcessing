import pandas as pd
import numpy as np
import json
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import MinMaxScaler
from umap import UMAP
import mysql.connector
import os

topic_model = BERTopic.load("./model/visualization_model")
fig = topic_model.visualize_topics()
fig.write_html("./vis_output2/topics_visualizationv1.html")