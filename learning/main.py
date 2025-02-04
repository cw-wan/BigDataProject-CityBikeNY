import os
import pandas as pd
import numpy as np

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, median_absolute_error
from xgboost import XGBRegressor

def add_time_features(df):
    """
    Generate time-based features from the 'time' column:
    - hour, day_of_week, month, is_weekend
    """
    df['time'] = pd.to_datetime(df['time'], errors='coerce', utc=True)
    df['hour'] = df['time'].dt.hour
    # dt.dayofweek: Monday=0, Sunday=6
    df['day_of_week'] = df['time'].dt.weekday
    df['month'] = df['time'].dt.month
    df['is_weekend'] = np.where(df['day_of_week'] >= 5, 1, 0)
    return df

def add_lag_and_rolling_features(df):
    """
    Group by 'zone_id' and generate time series features after sorting by time:
      - Lag features: lag_1, lag_24, lag_168
      - Rolling statistics: rolling_3h_mean (3-hour moving average), rolling_24h_max (24-hour max value)
      - Temporal change features: delta_1h = label - lag_1
    """
    def calc_features(group):
        group = group.sort_values('time').copy()
        group['lag_1'] = group['label'].shift(1)
        group['lag_24'] = group['label'].shift(24)
        group['lag_168'] = group['label'].shift(168)
        group['rolling_3h_mean'] = group['label'].rolling(window=3, min_periods=1).mean()
        group['rolling_24h_max'] = group['label'].rolling(window=24, min_periods=1).max()
        group['delta_1h'] = group['label'] - group['lag_1']
        return group

    df = df.groupby('zone_id', group_keys=False).apply(calc_features)
    return df

def main():
    # -------------------------------
    # 1. Load dataset
    # -------------------------------
    data_path = "data/citybike-ts.csv"
    df = pd.read_csv(data_path)

    # Generate label = demand - supply and remove demand and supply columns
    df['label'] = df['demand'] - df['supply']
    df.drop(columns=['demand', 'supply'], inplace=True)

    # -------------------------------
    # 2. Time feature engineering
    # -------------------------------
    df = add_time_features(df)

    # -------------------------------
    # 3. Process categorical features: Encode 'weather_main' as numerical values
    # -------------------------------
    le = LabelEncoder()
    df['weather_main_index'] = le.fit_transform(df['weather_main'])
    df.drop(columns=['weather_main', 'weather_description'], inplace=True)

    # -------------------------------
    # 4. Time series feature engineering
    # -------------------------------
    df = add_lag_and_rolling_features(df)

    # -------------------------------
    # 5. Select features and remove missing values
    # -------------------------------
    feature_cols = [
        "zone_id", "zone_lat", "zone_lng",
        "temp", "visibility", "wind_speed", "weather_main_index",
        "hour", "day_of_week", "month", "is_weekend",
        "lag_1", "lag_24", "lag_168", "rolling_3h_mean", "rolling_24h_max", "delta_1h"
    ]
    # Keep 'time' column for time-based splitting later
    cols_to_keep = ['time', 'label'] + feature_cols
    df = df[cols_to_keep]

    # Remove missing values caused by lag/rolling calculations
    df.dropna(inplace=True)

    # -------------------------------
    # 6. Split data
    # -------------------------------
    df.sort_values('time', inplace=True)
    split_time = pd.Timestamp("2023-10-01 00:00:00", tz="UTC")
    train_df = df[df['time'] < split_time].copy()
    test_df = df[df['time'] >= split_time].copy()

    print("Training set time range: {} ~ {}".format(train_df['time'].min(), train_df['time'].max()))
    print("Test set time range: {} ~ {}".format(test_df['time'].min(), test_df['time'].max()))

    X_train = train_df[feature_cols]
    y_train = train_df['label']
    X_test = test_df[feature_cols]
    y_test = test_df['label']

    # -------------------------------
    # 7. Configure and train XGBoost model
    # -------------------------------
    params = {
        "objective": "reg:squarederror",
        "n_estimators": 400,
        "max_depth": 9,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "gamma": 0.1,
        "tree_method": "hist",
        "missing": 0.0
    }

    print("Starting training!")
    model = XGBRegressor(**params)
    model.fit(X_train, y_train)

    # -------------------------------
    # 8. Prediction and evaluation
    # -------------------------------
    predictions = model.predict(X_test)
    # Compute evaluation metrics
    rmse = np.sqrt(mean_squared_error(y_test, predictions))  # Root Mean Squared Error
    mae = mean_absolute_error(y_test, predictions)  # Mean Absolute Error
    r2 = r2_score(y_test, predictions)  # R² Score
    median_ae = median_absolute_error(y_test, predictions)  # Median Absolute Error

    # Print evaluation results
    print("Evaluation Metrics:")
    print(f"Root Mean Squared Error (RMSE)  = {rmse:.4f}")
    print(f"Mean Absolute Error (MAE)      = {mae:.4f}")
    print(f"R² Score                       = {r2:.4f}")
    print(f"Median Absolute Error          = {median_ae:.4f}")

    # -------------------------------
    # 9. Save model
    # -------------------------------
    model_dir = "models"
    os.makedirs(model_dir, exist_ok=True)
    output_model_path = os.path.join(model_dir, "citybike-xgboost.model")
    model.save_model(output_model_path)
    print("Model saved to:", output_model_path)

    """
    Results:
    - Main -
    Root Mean Squared Error (RMSE)  = 4.8112
    Mean Absolute Error (MAE)      = 0.8511
    R² Score                       = 0.9815
    Median Absolute Error          = 0.1644
    - w/o weather - 
    Root Mean Squared Error (RMSE)  = 4.8921
    Mean Absolute Error (MAE)      = 0.9063
    R² Score                       = 0.9809
    Median Absolute Error          = 0.1831
    """


if __name__ == "__main__":
    main()
