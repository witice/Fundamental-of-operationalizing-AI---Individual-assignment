# Handle missing values
# For few missing data -> remove the records
df = df.dropna(subset=['PT08.S1(CO)', 'C6H6(GT)', 'PT08.S2(NMHC)', 'PT08.S3(NOx)', 'PT08.S4(NO2)', 'PT08.S5(O3)', 'T', 'RH', 'AH'])
missing_counts = df.isna().sum()
print(missing_counts)

# Total records
total_records = df.shape[0]
print(f"Total records: {total_records}")

# Replace missing values with mean
for column in ['CO(GT)', 'NOx(GT)', 'NO2(GT)']:
    df[column] = df[column].fillna(df[column].mean())
missing_counts = df.isna().sum()
print(missing_counts)

# Total records
total_records = df.shape[0]
print(f"Total records: {total_records}")

# Drop the column
df = df.drop(columns=['NMHC(GT)'])
missing_counts = df.isna().sum()
print(missing_counts)