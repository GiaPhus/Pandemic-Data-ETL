import dash
from dash import dcc, html, Input, Output
import dash_leaflet as dl
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px

# Đọc dữ liệu
df1 = pd.read_csv('./global_infos_df.csv')
df2 = pd.read_csv('./total_infos_df.csv')

df1['Date'] = pd.to_datetime(df1['Date'])

# Lọc dữ liệu trong khoảng thời gian từ 22/01/2020 đến 01/05/2021
start_date = '2020-01-22'
end_date = '2021-05-01'
df1 = df1[(df1['Date'] >= start_date) & (df1['Date'] <= end_date)]

# Sắp xếp dữ liệu theo cột Date
df1.sort_values(by='Date', inplace=True)

# Chuyển đổi cột Date về định dạng chuỗi để hiển thị
df1['Date'] = df1['Date'].dt.strftime('%m/%d/%Y')

# Tạo danh sách các Marker cho mỗi điểm dữ liệu
def create_markers(date):
    filtered_df = df1[df1['Date'] == date]
    markers = [
        dl.Marker(position=[row['Lat'], row['Long']],
                  children=dl.Tooltip(f"Region: {row['region']}\nConfirmed: {row['Cofirmed']}\nDeaths: {row['Death']}\nRecovered: {row['Recovered']}"))
        for index, row in filtered_df.iterrows()
    ]
    return markers

# Tạo biểu đồ Treemap
fig_treemap = px.treemap(df1, path=['WHO_Region'], values='Cofirmed',
                         color='Cofirmed', hover_data=['region'],
                         color_continuous_scale='matter', title='Current share of Worldwide COVID19 Confirmed Cases')
styled_table = df2.style.background_gradient(cmap="Reds")
# Tạo ứng dụng Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.ZEPHYR])

# Layout của ứng dụng Dash

app.layout = html.Div(children=[
    dbc.Navbar(id='navbar', children=[
        html.A(
            dbc.Row([
                dbc.Col(dbc.NavbarBrand("Covid-19 time_series infos", style={'color': 'black', 'fontSize': '25px'}))
            ], align="center", )),
        dbc.Row([
            dbc.Col(dbc.Button(id='button', children="Reload Data", color="primary", className='ml-auto', href='/'))],
            className="g-0 ms-auto flex-nowrap mt-3 mt-md-0")
    ]),
    html.Div(children=[
        dcc.Dropdown(
            id='date-dropdown',
            options=[{'label': date, 'value': date} for date in df1['Date'].unique()],
            value=df1['Date'].min(),
            clearable=False,
            style={'width': '50%', 'margin': '20px auto'}
        ),
        dl.Map(style={'width': '100%', 'height': '500px'}, center=[0, 0], zoom=2, id='map-1', children=[
            dl.TileLayer(),
            dl.LayerGroup(id='layer-1')
        ])
    ]),
    html.Div(children=[
        dcc.Graph(id='treemap', figure=fig_treemap),  # Thêm biểu đồ Treemap vào layout
    ], style={'width': '100%', 'margin': '20px auto'}),
    html.Div(children=[
        dcc.Dropdown(
            id='country-dropdown',
            options=[{'label': region, 'value': region} for region in df2['region'].unique()],
            value=df2['region'].iloc[0],
            clearable=False,
            style={'width': '50%', 'margin': '20px auto'}
        ),
        html.Div(id='country-info')
    ])
])

# Callback để cập nhật bản đồ khi chọn ngày
@app.callback(
    Output('layer-1', 'children'),
    Input('date-dropdown', 'value')
)
def update_map(date):
    return create_markers(date)

# Callback để hiển thị thông tin của quốc gia được chọn từ dropdown
@app.callback(
    Output('country-info', 'children'),
    Input('country-dropdown', 'value')
)
def display_country_info(country):
    # Tìm dữ liệu của quốc gia được chọn từ dataframe
    country_data = df2[df2['region'] == country]
    # Tạo HTML để hiển thị thông tin của quốc gia
    country_info_html = html.Div([
        html.H2(f"Thông tin của {country}"),
        html.P(f"Số ca nhiễm: {country_data['total_Confirmed'].iloc[0]}"),
        html.P(f"Số ca tử vong: {country_data['total_Death'].iloc[0]}"),
        html.P(f"Số ca hồi phục: {country_data['total_Recovered'].iloc[0]}"),
        html.P(f"WHO: {country_data['WHO_Region'].iloc[0]}")
    ])
    return country_info_html

# Chạy ứng dụng
if __name__ == '__main__':
    app.run_server(debug=False,host='0.0.0.0')
