import dash
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
import psycopg2

# configs
dbname = "order_books"
username = "postgres"
password = ""
kafka_host = "PLAINTEXT://ip-10-0-0-13.ec2.internal:9092"
db_host = "ip-10-0-0-11.ec2.internal"
hadoop_host = "ip-10-0-0-8.ec2.internal"

exchange_map = {'bitfinex': 'Bitfinex'}
exchange_options = [{'label': k, 'value': v} for k, v in exchange_map.items()]
default_exchange = 'bitfinex'


def generate_pairs(exchange):
    with open(f'./trading_pairs/{exchange}.pair', 'r') as f:
        pairs = ['t' + e.replace('\n', '') for e in f.readlines()]
    return pairs


trading_pairs = {exchange: generate_pairs(exchange) for exchange in exchange_map}

# Connect to server
conn = psycopg2.connect(host=db_host, database=dbname, user=username, password=password)
cur = conn.cursor()

app = dash.Dash("new orders")
app.layout = html.Div(
    [
        html.H4('new orders'),
        html.Div(
            children=[
                dcc.Dropdown(
                    clearable=False,
                    id='exchange-dropdown',
                    options=exchange_options,
                    value=default_exchange
                ),
                dcc.Dropdown(
                    clearable=False,
                    id='pair-dropdown',
                    value='BTCUSD'
                )
            ]
        ),
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='graph-update',
            interval=1 * 1000,
            n_intervals=0
        ),
    ]
)


@app.callback(
    Output('pair-dropdown', 'options'),
    [Input('exchange-dropdown', 'value')]
)
def update_pairs(dropdown_exchange):
    options = [{'label': pair, 'value': pair} for pair in trading_pairs[dropdown_exchange]]
    return options


@app.callback(
    Output('live-graph', 'figure')
    [Input('graph-update', 'n_intervals'), Input('pair-dropdown', 'value'), Input('exchange-dropdown', 'value')]
)
def update_graph(n, pair, exchange):
    cur.execute(f"""
    SELECT time, price, quantity, side, whale_score FROM new_orders
    WHERE basequote = {pair} AND exchange = {exchange}
    ORDER BY time DESC LIMIT 10000
    """
                )
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=('time', 'price', 'quantity', 'side', 'whale_score'))
    df['whale_score'] = 1 if df['whale_score'] > 0 else 0
    df['side, whale'] = list(zip(df.side, df.whale_score))
    fig = px.scatter(df, x=df['time'], y=df['quantity'], color=df['side'], size=df['quantity'],
                     labels={'x': 'time', 'y': 'price'})
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
