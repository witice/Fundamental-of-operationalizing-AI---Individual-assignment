import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor

def load_and__clean_data(data_set):
    # Load dataset
    df = pd.read_csv("AirQualityUCI.csv", sep = ";", decimal=',')

    # print(df.shape)
    # print(df.head())

    # Clean input table
    df = df.drop(columns=["Unnamed: 15", "Unnamed: 16"])
    # print(df.head())

    # Check missing NA
    missing_counts = df.isna().sum()
    # print(missing_counts)

    # print(df.isna())

    # Remove NAs
    df = df.dropna()

    missing_counts = df.isna().sum()
    # print(missing_counts)
    # Total records
    total_records = df.shape[0]
    # print(f"Total records: {total_records}")

    # Check missing values
    missing_counts = (df == -200).sum()
    # print(missing_counts)
    # Transform missing values -200 to NAs
    df.replace(-200, pd.NA, inplace=True)
    missing_counts = df.isna().sum()
    # print(missing_counts)

    # Handle missing values
    # For few missing data -> remove the records
    df = df.dropna(subset=['PT08.S1(CO)', 'C6H6(GT)', 'PT08.S2(NMHC)', 'PT08.S3(NOx)', 'PT08.S4(NO2)', 'PT08.S5(O3)', 'T', 'RH', 'AH'])
    missing_counts = df.isna().sum()
    # print(missing_counts)
    # Total records
    total_records = df.shape[0]
    # print(f"Total records: {total_records}")

    # Replace missing values with mean
    for column in ['CO(GT)', 'NOx(GT)', 'NO2(GT)']:
        df[column] = df[column].fillna(df[column].mean())
    missing_counts = df.isna().sum()
    # print(missing_counts)
    # Total records
    total_records = df.shape[0]
    # print(f"Total records: {total_records}")
    # Drop the column
    df = df.drop(columns=['NMHC(GT)'])
    missing_counts = df.isna().sum()
    # print(missing_counts)

    return df

def format_data(data_set):
    df = data_set
    # Format date
    df['DateTime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].str.replace('.', ':'), format='%d/%m/%Y %H:%M:%S')

    df.set_index('DateTime')

    # Extract time-based features
    df['year'] = df['DateTime'].dt.year
    df['month'] = df['DateTime'].dt.month
    df['day'] = df['DateTime'].dt.day
    df['day_of_week'] = df['DateTime'].dt.dayofweek
    df['day_of_year'] = df['DateTime'].dt.dayofyear
    df['hour'] = df['DateTime'].dt.hour

    # Creating lag features for CO, NOx, and Benzene
    for lag in [1, 2, 3, 6, 12, 24]:
        df[f"CO_lag_{lag}"] = df["CO(GT)"].shift(lag)
        df[f"NOx_lag_{lag}"] = df["NOx(GT)"].shift(lag)
        df[f"Benzene_lag_{lag}"] = df["C6H6(GT)"].shift(lag)

    # Creating rolling statistics (rolling mean and std deviation)
    for window in [3, 6, 12]:
        df[f"CO_roll_mean_{window}"] = df["CO(GT)"].rolling(window=window).mean()
        df[f"CO_roll_std_{window}"] = df["CO(GT)"].rolling(window=window).std()
        df[f"NOx_roll_mean_{window}"] = df["NOx(GT)"].rolling(window=window).mean()
        df[f"NOx_roll_std_{window}"] = df["NOx(GT)"].rolling(window=window).std()
        df[f"Benzene_roll_mean_{window}"] = df["C6H6(GT)"].rolling(window=window).mean()
        df[f"Benzene_roll_std_{window}"] = df["C6H6(GT)"].rolling(window=window).std()

    df.fillna(method='bfill', inplace=True)

def linear_regression_train_model(target_pollutant, features):
    df = load_and__clean_data(pd.read_csv("AirQualityUCI.csv", sep = ";", decimal=','))
    format_data(df)
    # Model prediction
    # Fit model for target pollutant

    X = df[features]
    y = df[target_pollutant]

    # Split Data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LinearRegression()
    return model.fit(X_train, y_train)

def random_forest_train_model(target_pollutant, features):
    df = load_and__clean_data(pd.read_csv("AirQualityUCI.csv", sep = ";", decimal=','))
    format_data(df)
    # Model prediction
    # Fit model for target pollutant

    X = df[features]
    y = df[target_pollutant]

    # Split Data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    return model.fit(X_train, y_train)

def model_predict(model, X):
    # Predict
    return model.predict(X)

# Train and Evaluate Models
def evaluate_model(model, X_test, y_test, model_name):
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    print(f"{model_name} - MSE: {mse:.2f}, MAE: {mae:.2f}")

