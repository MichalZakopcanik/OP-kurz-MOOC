# Ziskane data z New York City 311 Service Requests dataset je priamo z linky: 
# https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/explore/query/SELECT%0A%20%20%60unique_key%60%2C%0A%20%20%60created_date%60%2C%0A%20%20%60closed_date%60%2C%0A%20%20%60agency%60%2C%0A%20%20%60agency_name%60%2C%0A%20%20%60complaint_type%60%2C%0A%20%20%60descriptor%60%2C%0A%20%20%60descriptor_2%60%2C%0A%20%20%60location_type%60%2C%0A%20%20%60incident_zip%60%2C%0A%20%20%60incident_address%60%2C%0A%20%20%60street_name%60%2C%0A%20%20%60cross_street_1%60%2C%0A%20%20%60cross_street_2%60%2C%0A%20%20%60intersection_street_1%60%2C%0A%20%20%60intersection_street_2%60%2C%0A%20%20%60address_type%60%2C%0A%20%20%60city%60%2C%0A%20%20%60landmark%60%2C%0A%20%20%60facility_type%60%2C%0A%20%20%60status%60%2C%0A%20%20%60due_date%60%2C%0A%20%20%60resolution_description%60%2C%0A%20%20%60resolution_action_updated_date%60%2C%0A%20%20%60community_board%60%2C%0A%20%20%60council_district%60%2C%0A%20%20%60police_precinct%60%2C%0A%20%20%60bbl%60%2C%0A%20%20%60borough%60%2C%0A%20%20%60x_coordinate_state_plane%60%2C%0A%20%20%60y_coordinate_state_plane%60%2C%0A%20%20%60open_data_channel_type%60%2C%0A%20%20%60park_facility_name%60%2C%0A%20%20%60park_borough%60%2C%0A%20%20%60vehicle_type%60%2C%0A%20%20%60taxi_company_borough%60%2C%0A%20%20%60taxi_pick_up_location%60%2C%0A%20%20%60bridge_highway_name%60%2C%0A%20%20%60bridge_highway_direction%60%2C%0A%20%20%60road_ramp%60%2C%0A%20%20%60bridge_highway_segment%60%2C%0A%20%20%60latitude%60%2C%0A%20%20%60longitude%60%2C%0A%20%20%60location%60%0AWHERE%0A%20%20%60created_date%60%0A%20%20%20%20BETWEEN%20%222024-01-01T00%3A00%3A00%22%20%3A%3A%20floating_timestamp%0A%20%20%20%20AND%20%222025-11-01T02%3A45%3A00%22%20%3A%3A%20floating_timestamp%0AORDER%20BY%20%60created_date%60%20DESC%20NULL%20FIRST/page/filter



import pandas as pd
import dash
from dash import html
from dash import dcc
from dash.dependencies import Input, Output
import plotly.express as px


ba2024_cat_complaints_df = pd.read_csv("baComplaintCategoriesCount2024.csv")
ba2024_complaints_sum = ba2024_cat_complaints_df["Count"].sum()
ba2025_cat_complaints_df = pd.read_csv("baComplaintCategoriesCount2025.csv")
ba2025_complaints_sum = ba2025_cat_complaints_df["Count"].sum()
ba2024_month_complaints_df = pd.read_csv("complaintsByMonthBA2024.csv")
ba2025_month_complaints_df = pd.read_csv("complaintsByMonthBA2025.csv")

nyc2024_cat_complaints_df = pd.read_csv("nycComplaintCategoriesCount2024.csv")
nyc2024_complaints_sum = nyc2024_cat_complaints_df["Count"].sum()
nyc2025_cat_complaints_df = pd.read_csv("nycComplaintCategoriesCount2025.csv")
nyc2025_complaints_sum = nyc2025_cat_complaints_df["Count"].sum()
nyc2024_month_complaints_df = pd.read_csv("complaintsByMonthNYC2024.csv")
nyc2025_month_complaints_df = pd.read_csv("complaintsByMonthNYC2025.csv")

ba_population = 478040
nyc_population = 8478000

theoretical_ba2025_nov_dec_complaints_sum = ba2024_month_complaints_df[(ba2024_month_complaints_df["Month"] == "November")]["Complaint Count"].iloc[0]/2 + ba2024_month_complaints_df[(ba2024_month_complaints_df["Month"] == "December")]["Complaint Count"].iloc[0]
theoretical_ba2025_complaints_sum = ba2025_complaints_sum + theoretical_ba2025_nov_dec_complaints_sum

theoretical_nyc2025_nov_dec_complaints_sum = nyc2024_month_complaints_df[(nyc2024_month_complaints_df["Month"] == "November")]["Complaint Count"].iloc[0] + nyc2024_month_complaints_df[(nyc2024_month_complaints_df["Month"] == "December")]["Complaint Count"].iloc[0]
theoretical_nyc2025_complaints_sum = nyc2025_complaints_sum + theoretical_nyc2025_nov_dec_complaints_sum

def get_count_bar_chart_NYC():
        
        complaint_amount_df = pd.DataFrame({
             "Rok": ["2024", "2025", "Teoretický 2025"],
             "Počet sťažností": [nyc2024_complaints_sum, nyc2025_complaints_sum, theoretical_nyc2025_complaints_sum]
        })
        fig = px.bar(complaint_amount_df, x="Rok", y="Počet sťažností",
             color="Rok", barmode='group', title = 'Počet sťažností v New York City')
        fig.update_layout(bargap=0.2)
        return fig


def get_count_bar_chart_BA():
        
        complaint_amount_df = pd.DataFrame({
             "Rok": ["2024", "2025", "Teoretický 2025"],
             "Počet sťažností": [ba2024_complaints_sum, ba2025_complaints_sum, theoretical_ba2025_complaints_sum]
        })
        fig = px.bar(complaint_amount_df, x="Rok", y="Počet sťažností",
             color="Rok", barmode='group', title = 'Počet sťažností v Bratislave')
        fig.update_layout(bargap=0.2)
        return fig


def get_month_complaint_bar_chart_NYC():
        df2024 = nyc2024_month_complaints_df.copy()
        df2024["Rok"] = "2024"
        df2025 = nyc2025_month_complaints_df.copy()
        df2025["Rok"] = "2025"
        complaint_amount_df = pd.concat(
             [df2024, df2025]
        )
        complaint_amount_df.rename(columns={"Complaint Count": "Počet sťažností", "Month": "Mesiac"}, inplace=True)
        fig = px.bar(complaint_amount_df, x="Mesiac", y="Počet sťažností",
             color="Rok", barmode='group', title = 'Počet sťažností v New York City')
        fig.update_layout(bargap=0.2)
        return fig


def get_month_complaint_bar_chart_BA():
        
        df2024 = ba2024_month_complaints_df.copy()
        df2024["Rok"] = "2024"
        df2025 = ba2025_month_complaints_df.copy()
        df2025["Rok"] = "2025"
        complaint_amount_df = pd.concat(
             [df2024, df2025]
        )
        complaint_amount_df.rename(columns={"Complaint Count": "Počet sťažností", "Month": "Mesiac"}, inplace=True)
        fig = px.bar(complaint_amount_df, x="Mesiac", y="Počet sťažností",
             color="Rok", barmode='group', title = 'Počet sťažností v Bratislave')
        fig.update_layout(bargap=0.2)
        return fig


app = dash.Dash(__name__)

app.layout = html.Div(style={"padding": "10px", "width": "80%", "margin": "auto"},
                      children=[html.H1('Porovnanie sťažností v NYC a BA',
                                        style={'textAlign': 'center', 'color': '#503D36',
                                               'font-size': 40}),
                                
                                dcc.Dropdown(id='year-dropdown',
                                                options=[{'label': '2024', 'value': '2024'}] + [{'label': '2025', 'value': '2025'}],
                                                value='2024',
                                                placeholder="Vyber si rok pre štatistiku",
                                                searchable=True,
                                                style={"width": "40%"}),
                                    html.Br(),

                                html.Div([dcc.Graph(id='nyc-categories-pie-chart'),
                                         dcc.Graph(id='ba-categories-pie-chart')]),
                                html.Br(),
                                html.Div([
                                    html.Div(dcc.Graph(figure = get_count_bar_chart_NYC()),
                                                    style={"width": "45%", "display": "inline-block"}
                                                    ),
                                    html.Div(dcc.Graph(figure = get_count_bar_chart_BA()),
                                                    style={"width": "45%", "display": "inline-block"}),
                                ]),
                                html.Br(),
                                html.Br(),
                                html.Table(
                                    className="complaints-table",
                                    children=[
                                    html.Caption("Počet sťažností na občana v jednotlivých mestách"),
                                    html.Thead([
                                        html.Tr([
                                            html.Th("Mesto"),
                                            html.Th("2024"),
                                            html.Th("2025")
                                        ])
                                    ]),
                                    html.Tbody([
                                        html.Tr([
                                            html.Td("NYC"),
                                            html.Td((nyc2024_complaints_sum/nyc_population).round(4)),
                                            html.Td((nyc2025_complaints_sum/nyc_population).round(4))
                                        ]),
                                        html.Tr([
                                            html.Td("BA"),
                                            html.Td((ba2024_complaints_sum/ba_population).round(4)),
                                            html.Td((ba2025_complaints_sum/ba_population).round(4))
                                        ]),
                                    ])
                                ]),
                                html.Br(),
                                html.Div([
                                    html.Div(dcc.Graph(figure = get_month_complaint_bar_chart_NYC()),
                                                    style={"width": "45%", "display": "inline-block"}
                                                    ),
                                    html.Div(dcc.Graph(figure = get_month_complaint_bar_chart_BA()),
                                                    style={"width": "45%", "display": "inline-block"}),
                                ]),
                                html.Br(),
                            ])

@app.callback(Output(component_id='nyc-categories-pie-chart', component_property='figure'),
              Input(component_id='year-dropdown', component_property='value'))
def get_categories_pie_chart_NYC(entered_year):
        if entered_year == '2024':
            nyc_cat_complaints_df = nyc2024_cat_complaints_df
            nyc_complaints_sum = nyc2024_complaints_sum
        elif entered_year == '2025':
            nyc_cat_complaints_df = nyc2025_cat_complaints_df
            nyc_complaints_sum = nyc2025_complaints_sum
        else:
            nyc_cat_complaints_df = (
                 pd.concat([nyc2024_cat_complaints_df, nyc2025_cat_complaints_df])
                 .groupby("Complaint Type", as_index=False)
                 .sum()
            )
            nyc_complaints_sum = nyc2024_complaints_sum + nyc2025_complaints_sum

        nyc_others_sum = nyc_complaints_sum - nyc_cat_complaints_df.head(15)["Count"].sum()
        nyc_cat_complaints_df = pd.concat([nyc_cat_complaints_df.head(15), pd.DataFrame([["Others", nyc_others_sum]], columns=["Complaint Type", "Count"])], ignore_index=True)
        fig = px.pie(nyc_cat_complaints_df, values='Count', 
        names='Complaint Type', 
        title='Kategórie sťažností v New York City')
        return fig

@app.callback(Output(component_id='ba-categories-pie-chart', component_property='figure'),
              Input(component_id='year-dropdown', component_property='value'))
def get_categories_pie_chart_BA(entered_year):
        if entered_year == '2024':
            ba_cat_complaints_df = ba2024_cat_complaints_df
        elif entered_year == '2025':
            ba_cat_complaints_df = ba2025_cat_complaints_df
        else:
            ba_cat_complaints_df = (
                 pd.concat([ba2024_cat_complaints_df, ba2025_cat_complaints_df])
                 .groupby("Category", as_index=False)
                 .sum()
            )
        fig = px.pie(ba_cat_complaints_df, values='Count', 
        names='Category', 
        title='Kategórie sťažností v Bratislave')
        return fig


if __name__ == '__main__':
    app.run()
