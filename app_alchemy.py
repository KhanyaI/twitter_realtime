import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import nltk
import plotly
import sqlite3
nltk.download('wordnet')
nltk.download('words')

words = set(nltk.corpus.words.words())
lemz = nltk.stem.wordnet.WordNetLemmatizer()
import re

def preprocessing(tweet):
    
    lower = tweet.lower()
    split = lower.split()
    
    characters =  [t for t in split if re.match(r'[^\W\d]*$', t)]
    
    clean = [word for word in characters if word not in nltk.corpus.stopwords.words('english')]
    
    cleaner = [word for word in clean if word in words]
    
    normalized = []
    for word in cleaner:
        normalize = lemz.lemmatize(word)
        normalized.append(normalize)


    all_words = ' '.join([text for text in normalized])
    

    return all_words, normalized

def plotting(normalized):
    fdist = nltk.probability.FreqDist(normalized)
    fdist_df = pd.DataFrame(fdist.most_common(11),columns = ["Word","Frequency"]).drop([0]).reindex()
    return fdist_df

def read_and_plot():

    con = sqlite3.connect('/app/stream/tweets.sqlite')
    #con = sqlite3.connect('path/tweets.sqlite')

    df = pd.read_sql('SELECT * FROM tweet LIMIT 100', con = con)
    #print(df.shape)        
    words = []
    normal_list = []

    for t in df.text[:10]:
        
        all_joint, normal = preprocessing(t)

        words.append(all_joint)
        normal_list.append(normal)

    
    flat_normal_list = [item for sublist in normal_list for item in sublist]
    #print(flat_normal_list)

    df = plotting(flat_normal_list)
    return df

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__,external_stylesheets=external_stylesheets)
#app.css.append_css({"external_url" : "https://codepen.io/chriddyp/pen/bWLwgP.css"})

colors = {
    'background': '#ffffff',
    'text': '#9467bd'
}

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(children='Fun twitter analysis',
    	style={
    	'textAlign': 'center',
    	'color': colors['text']
    	}
    ),

    html.Div(children='What happens when you self-teach python and work in marketing', style={
    	'textAlign': 'center',
    	'color': colors['text']
    	}),

    dcc.Graph(id='main_graph'),

    dcc.Interval(
        id="query_update",interval=6000,n_intervals=0),

])
@app.callback(Output('main_graph', 'figure'),Input('query_update', 'n_intervals'))
def update_figure(n):
    df = read_and_plot()
    fig = px.bar(df, x='Word', y='Frequency')
    fig.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_family="Times New Roman",
    title_font_color="black")
    fig.update_traces(marker_color='MediumPurple')
    return fig


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0',port=8080)
