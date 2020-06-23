import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.express as px
import psycopg2
from dash.dependencies import Output, Input

# configs
dbname = "order_books"
username = "postgres"
password = ""
kafka_host = "PLAINTEXT://ip-10-0-0-13.ec2.internal:9092"
db_host = "ec2-35-172-82-16.compute-1.amazonaws.com"
hadoop_host = "ip-10-0-0-8.ec2.internal"

exchange_map = {'Bitfinex': 'Bitfinex', 'Binance.com': 'binance'}
exchange_options = [{'label': k, 'value': v} for k, v in exchange_map.items()]
default_exchange = 'Bitfinex'
btcusd_default = {'Bitfinex': 'tBTCUSD', 'binance': 'BTCUSDT'}


# print(exchange_options)


def generate_pairs(exchange):
    with open(f'./trading_pairs/{exchange}.pair', 'r') as f:
        pairs = [e.replace('\n', '') for e in f.readlines()]
    if exchange == 'Bitfinex':
        pairs = list(map(lambda x: 't' + x, pairs))
    return pairs


trading_pairs = {exchange: generate_pairs(exchange) for exchange in exchange_map.values()}

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
                    value='tBTCUSD'
                ),
                html.Button(children='stop', id='update-control')
            ]
        ),
        dcc.Graph(id='live-graph'),
        dcc.Interval(
            id='graph-update',
            interval=500,
            n_intervals=0,
            disabled=False
        ),
    ]
)


@app.callback(
    Output('update-control', 'children'),
    [Input('update-control', 'n_clicks')]
)
def update_control(clicks):
    return 'stop' if not clicks or clicks % 2 == 0 else 'start'


@app.callback(
    Output('graph-update', 'disabled'),
    [Input('update-control', 'children')]
)
def update_rolling(next_action):
    return next_action == 'start'


@app.callback(
    Output('pair-dropdown', 'value'),
    [Input('exchange-dropdown', 'value')]
)
def update_defaultPair(dropdown_exchange):
    return btcusd_default[dropdown_exchange]


@app.callback(
    Output('pair-dropdown', 'options'),
    [Input('exchange-dropdown', 'value')]
)
def update_pairs(dropdown_exchange):
    options = [{'label': pair, 'value': pair} for pair in trading_pairs[dropdown_exchange]]
    # print(dropdown_exchange, trading_pairs)
    return options


@app.callback(
    Output('live-graph', 'figure'),
    [Input('graph-update', 'n_intervals'), Input('pair-dropdown', 'value'), Input('exchange-dropdown', 'value')]
)
def update_graph(n, pair, exchange):
    # print(pair, exchange)
    cur.execute(f"""
    SELECT time, price, quantity, side, whale_score,avg FROM new_orders
    WHERE basequote = '{pair}' AND exchange = '{exchange}'
    ORDER BY time DESC LIMIT 100
    """
                )
    rows = cur.fetchall()
    if not rows:
        return px.scatter(labels={'x': 'time', 'y': 'price'})
    # if rows: print(rows[0])
    # if not rows: print("e")
    df = pd.DataFrame(rows, columns=('time', 'price', 'quantity', 'side', 'whale_score', 'avg'))
    # df['whale_score'] = 1 if df['whale_score'] > 0 else 0
    df['whale_score'] = (df['whale_score'] > 0).astype(int)
    df['side_whale'] = list(zip(df.side, df.whale_score))
    # print(len(list(df.quantity)))
    fig = px.scatter(df, x=df.time, y=df.price, color=df.side_whale, size=df.quantity,
                     labels={'x': 'time', 'y': 'price'},
                     color_discrete_map={(-1, 0): "red", (1, 0): "green", (-1, 1): "blue", (1, 1): "blue"})
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
