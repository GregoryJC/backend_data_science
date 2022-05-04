# %%
import pandas as pd
import numpy as np
import os

import plotly.graph_objects as go

# %% Get geoplot
def get_geoplot(y : pd.core.frame.DataFrame, title_string : str):
    """
    A method to plot geo data mapped on a open-street-map layer

    Parameter
    ----------
    y : pandas.core.frame.DataFrame
        a pandas dataframe containing the geo data as 'lat' and 'lon' columns
    title_string : str
        the name of plot to be stored in folder 'visualizations/

    Return
    ----------
    fig : plotly.graph_objs._figure.Figure
        a tree-like data structure rendered by the plotly.js JavaScript library
    """

    if "lat" in y and "lon" in y:

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

        return fig
        
# %% Get monthly parts of the data
def get_month(y : pd.core.frame.DataFrame, month : str):
    """
    A method to filter the data by a given month

    Parameter
    ----------
    y : pandas.core.frame.DataFrame
        a pandas dataframe containing the month data as 'date' column
    month : str
        the name of month for wich the data should be filtered

    Return
    ----------
    y : pandas.core.frame.DataFrame
        a pandas dataframe containing filtered by the given month
    """

    return y[y["date"].str.contains(month)]

# %% Get min sum_raw_reads parts of the data
def get_min_sum_raw_reads(y : pd.core.frame.DataFrame, threshold : int): 
    """
    A method to filter the data by a given sum_raw_reads threshold

    Parameter
    ----------
    y : pandas.core.frame.DataFrame
        a pandas dataframe containing the sum_raw_reads data as 'sum_raw_reads' column
    threshold : int
        the threshold that states the minimum amount of sum_raw_reads for wich the data should be filtered

    Return
    ----------
    y : pandas.core.frame.DataFrame
        a pandas dataframe containing filtered by the given threshold
    """

    return y[y["sum_raw_reads"] >= threshold]
