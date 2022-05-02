import pandas as pd
import numpy as np
import os
import sys

import plotly.graph_objects as go

# %%
y =  pd.read_parquet("../data/y_example.parquet")

# %% Get geoplot
def get_geoplot(y):

    if "lat" in y and "lon" in y:

        title_string = f"Geoplot"

        fig = go.Figure(
            go.Scattermapbox(
                lat=y["lat"],
                lon=y["lon"],
                mode='markers',
                customdata = y.values,
                hovertemplate=' '.join([col + ": %{customdata[" + str(i) + "]}<br>" for i, col in enumerate(y)]) +
                    "<extra></extra>",       
            ))

        fig.update_traces(marker=dict(size=8))
        fig.update_layout(mapbox_style="open-street-map", 
                            mapbox_zoom=4, 
                            mapbox_center=go.layout.mapbox.Center(
                                            lat=50,
                                            lon=11
                ))
        fig.update_layout(legend=dict(
            yanchor="top",
            y=0.95
        ))
        fig.update_layout(margin={"r":100,"t":0,"l":0,"b":0})

        fig.write_html(f"../visualizations/{pd.Timestamp.now().strftime('%Y-%m-%d__%Hh%Mm')}__{title_string}.html")

# %% Get monthly parts of the data
def get_month(y, month): 

    return y[y["date"].str.contains(month)]

# %% Get min sum_raw_reads parts of the data
def get_min_sum_raw_reads(y, threshold): 

    return y[y["sum_raw_reads"] > threshold]
