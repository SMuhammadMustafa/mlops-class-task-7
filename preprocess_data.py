import pandas as pd

# Load the data
df = pd.read_csv('./data/weather_data.csv')

# Fill missing numerical values with the mean of the column
df['Temperature'] = df['Temperature'].fillna(df['Temperature'].mean())
df['Humidity'] = df['Humidity'].fillna(df['Humidity'].mean())
df['Wind Speed'] = df['Wind Speed'].fillna(df['Wind Speed'].mean())

# For categorical data like 'Weather Condition', you might fill with the mode
df['Weather Condition'] = df['Weather Condition'].fillna(df['Weather Condition'].mode()[0])

# Normalize Temperature and Wind Speed
df['Temperature'] = (df['Temperature'] - df['Temperature'].min()) / (df['Temperature'].max() - df['Temperature'].min())
df['Wind Speed'] = (df['Wind Speed'] - df['Wind Speed'].min()) / (df['Wind Speed'].max() - df['Wind Speed'].min())

# Standardize Temperature and Wind Speed
df['Temperature'] = (df['Temperature'] - df['Temperature'].mean()) / df['Temperature'].std()
df['Wind Speed'] = (df['Wind Speed'] - df['Wind Speed'].mean()) / df['Wind Speed'].std()

# Save the processed data
df.to_csv('./data/processed_data.csv', index=False)