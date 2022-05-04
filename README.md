# Filtering and Plotting an Example Dataset

The aim for this little task is to load in example data, then process it by filtering its values, and in the end plot the data as a scattermapbox.

## Data

The data sets for this task can be found in the folder [data](./data) and is divided into 3 separate files (.json, .parquet, .sqlite).

## Preprocessing

After loading the data in You should set up the possibility to filter the data by its 'date' and 'sum_raw_reads' columns independently. For this You can use the given scripts You may find in [data_processing](./src/data_processing.py).

## Visualization

In the end You should be plotting the data by using its geo data stored as 'lat' and 'lon' columns. For this You may use the given script which can be found in [data_processing](./src/data_processing.py) & add a local folder 'visualizations/' on the root level where to store the plots.

## Remarks

All the code given here is just a hint of the general structure. Feel perfectly free to rewrite the methods on Your own if You prefer to.