# Hourly Demand Forcasting System for NYC Bike Sharing Stations

Project for TSP-CSC5003 Big Data Infrastructures & semantic networks

**Author**: Chenwei Wan

## User Guide

1. Download [2023 NYC City Bike Trip Data](https://s3.amazonaws.com/tripdata/index.html) and [2023 New York weather data](https://www.kaggle.com/datasets/heqiang01/hourly-weather-data-in-2023-in-usa) (only use NewYorkWeatherData.xlsx, and you need to convert it to a csv file). Decompress and copy csv files to `$HDFS_BASE_PATH/raw/citybike` and `$HDFS_BASE_PATH/raw/weather` respectively.
2. **Data Processing**: Run `Dataleaner.scala` and `DataEnricher.scala` in `src/main/scala/preprocessing`.
3. **Learning**: Download time series data from `$HDFS_BASE_PATH/processed/zonets` to `learning/data` and rename the csv file to `citybike-ts.csv`. Then run `learning/main.py`.
4. **Web Application**: Run `src/main/scala/backend/MainApp.scala` and host `frontend/index.html` with any IDE like WebStorm or Intellij IDEA.

`$HDFS_BASE_PATH` is the entry point of your HDFS service, for example `hdfs://localhost:9000`.


## System Pipeline

![](pipeline.png)

## Interactive Visualization

![](Interface.png)
