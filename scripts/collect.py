import pandas as pd
import requests
import json
from datetime import datetime
import os

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Fetch data from API
url = 'https://data.ca.gov/api/3/action/datastore_search?resource_id=c8ca9eb7-d83b-44d6-95d4-56974082dc31'
response = requests.get(url)
data = json.loads(response.text)

# Convert to DataFrame
df = pd.json_normalize(data['result']['records'])

# Add timestamp when data was collected
collection_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df['collection_time'] = collection_time

# Generate filenames
csv_filename = f"data/power_outage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
master_csv = "data/power_outage_master.csv"

# Save current snapshot
df.to_csv(csv_filename, index=False)

# Check if master file exists and handle accordingly
if os.path.exists(master_csv):
    # Read existing master data
    master_df = pd.read_csv(master_csv)
    
    # Create a unique identifier for each outage record
    # Using IncidentID (or OBJECTID), StartDateTime, and UtilityCompany as composite key
    df['unique_key'] = df['IndicentID'].astype(str) + '_' + df['StartDateTime'].astype(str) + '_' + df['UtilityCompany'].astype(str)
    master_df['unique_key'] = master_df['IndicentID'].astype(str) + '_' + master_df['StartDateTime'].astype(str) + '_' + master_df['UtilityCompany'].astype(str)
    
    # Filter out records that already exist in the master file
    new_records = df[~df['unique_key'].isin(master_df['unique_key'])]
    
    # Remove the temporary unique_key column
    new_records = new_records.drop('unique_key', axis=1)
    
    # If there are new records, append them to the master file
    if not new_records.empty:
        new_records.to_csv(master_csv, mode='a', header=False, index=False)
        print(f"Added {len(new_records)} new records at {collection_time}")
    else:
        print(f"No new records to add at {collection_time}")
        
    # Clean up
    df = df.drop('unique_key', axis=1)
    
else:
    # If master file doesn't exist, create it
    df.to_csv(master_csv, index=False)
    print(f"Created master file with {len(df)} records at {collection_time}")

print(f"Data collection process completed at {collection_time}")