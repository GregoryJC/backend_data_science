import sqlite3
import pandas as pd
import plotly.graph_objects as go
from traceback import format_exc
from os.path import join, dirname, abspath


class Pipeline:
    def __init__(self, min_sum_raw_reads=0, month=''):
        self.min_sum_raw_reads = min_sum_raw_reads
        self.month = month
        self.root_path = dirname(dirname(abspath(__file__)))
        self.data_path = join(self.root_path, 'data')
        self.visualizations_path = join(self.root_path, 'visualizations')
        self.key_columns = ['lat', 'lon', 'date', 'sum_raw_reads', 'extra_target']
        self.df = self.load_data()

    def get_geoplot(self, y : pd.core.frame.DataFrame, title_string : str):
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
            fig_filename = f"{pd.Timestamp.now().strftime('%Y-%m-%d__%Hh%Mm')}__{title_string}.html"
            fig_filepath = join(self.visualizations_path, fig_filename)
            fig.write_html(fig_filepath)
            print(f"Plot saved as: '{fig_filepath}'\n")
            return fig
            

    def get_month(self, y : pd.core.frame.DataFrame, month : str):
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
        filtered_df = y[y["date"].str.contains(month)]
        filtered_df.to_excel(join(self.data_path, 'data_filtered_by_month.xlsx'))
        return filtered_df


    def get_min_sum_raw_reads(self, y : pd.core.frame.DataFrame, threshold : int): 
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
        filtered_df = y[y["sum_raw_reads"] >= threshold]
        filtered_df.to_excel(join(self.data_path, 'data_filtered_by_sum_raw_reads.xlsx'))
        return filtered_df


    def load_data(self): 
        """
        Load 3 datasets from '/data' and combine them into one dataframe

        example_ml_APRIL.json: 
        {
            "lat": "50.328093",
            "lon": "11.80973",
            "date": "APR-I",
            "sum_raw_reads": 15752,
            "dataset": "Example",
            "extra_target": "5636_4_G_gro\u00df_V3"
        }

        example_ml_II.sqlite - table_1: 
        (0, 'uuid', 'TEXT', 0, None, 0)
        (1, 'lat', 'TEXT', 0, None, 0)
        (2, 'lon', 'TEXT', 0, None, 0)
        (3, 'date', 'TEXT', 0, None, 0)
        (4, 'sum_raw_reads', 'INTEGER', 0, None, 0)
        (5, 'dataset', 'TEXT', 0, None, 0)
        (6, 'extra_target', 'TEXT', 0, None, 0)

        example_ml.parquet: 
        'lat', 'lon', 'date', 'sum_raw_reads', 'dataset', 'extra_target'
        """
        print(f"Loading datasets in '{self.data_path}'")
        print(f"Key columns = {self.key_columns}")

        # load json file
        json_df = pd.read_json(join(self.data_path, 'example_ml_APRIL.json'))
        
        # load sqlite file
        connection = sqlite3.connect(join(self.data_path, 'example_ml_II.sqlite'))
        sqlite_df = pd.read_sql_query("SELECT * from table_1", connection)
        connection.close()

        # load parquet file
        parquet_df = pd.read_parquet(join(self.data_path, 'example_ml.parquet'), engine='fastparquet')

        # combine 3 datasets into one dataframe
        self.df = pd.concat([json_df, sqlite_df, parquet_df])
        print(f"df.size = {self.df.size}")

        # remove duplicates
        self.df.drop_duplicates(subset=self.key_columns, inplace=True)
        print(f"Removed duplicates. df.size = {self.df.size}")

        # remove rows with null values in any key column
        self.df.dropna(subset=self.key_columns, how='any', axis=0, inplace=True) 
        print(f"Removed rows with any null value in key_columns. df.size = {self.df.size}\n")
        # combined_data_df.to_excel('data/combined_data.xlsx')
        return self.df


    def process_data(self): 
        """
        Load the datasets in 'data/' and process all the data.
        Plot the data as scattermapbox and save the plots in 'visualizations/'.
        """
        print(f"Processing data: min_sum_raw_reads={self.min_sum_raw_reads}, month={self.month}\n")
        try:
            filtered_df = self.get_min_sum_raw_reads(self.df, self.min_sum_raw_reads)
            filtered_df = self.get_month(filtered_df, self.month)
            title = f"in_{self.month}_min_sum_raw_reads_{self.min_sum_raw_reads}"
            self.get_geoplot(filtered_df, title)
        except:
            print(f"\n【Exception】{format_exc()}")


if __name__ == '__main__':
    # sum_raw_reads range: [744, 91285]
    # month options: 'APR', 'MAY', 'JULY', 'AUG'
    pipeline = Pipeline()    
    pipeline.min_sum_raw_reads = 10000
    for month in ['APR', 'MAY', 'JULY', 'AUG']:
        pipeline.month = month
        pipeline.process_data()
