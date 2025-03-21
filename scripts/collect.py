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

try:
    # Check if OBJECTID exists in the dataframe
    if 'OBJECTID' in df.columns:
        # Check if master file exists
        if os.path.exists(master_csv):
            # Read existing master data
            master_df = pd.read_csv(master_csv)
            
            # Convert OBJECTID to string in both dataframes
            df['OBJECTID_str'] = df['OBJECTID'].astype(str)
            
            # Create the same key in master_df if needed
            if 'OBJECTID_str' not in master_df.columns and 'OBJECTID' in master_df.columns:
                master_df['OBJECTID_str'] = master_df['OBJECTID'].astype(str)
            
            # Filter out records that already exist
            new_records = df[~df['OBJECTID_str'].isin(master_df['OBJECTID_str'])]
            
            # Remove the temporary column
            new_records = new_records.drop('OBJECTID_str', axis=1)
            
            # If there are new records, append them
            if not new_records.empty:
                new_records.to_csv(master_csv, mode='a', header=False, index=False)
                print(f"Added {len(new_records)} new records at {collection_time}")
            else:
                print(f"No new records to add at {collection_time}")
            
            # Clean up
            df = df.drop('OBJECTID_str', axis=1)
        else:
            # If master file doesn't exist, create it
            df.to_csv(master_csv, index=False)
            print(f"Created master file with {len(df)} records at {collection_time}")
    else:
        # If we can't find OBJECTID, fallback to appending all data
        print(f"WARNING: Could not find OBJECTID column. Appending all data without deduplication.")
        if os.path.exists(master_csv):
            df.to_csv(master_csv, mode='a', header=False, index=False)
        else:
            df.to_csv(master_csv, index=False)
        
except Exception as e:
    print(f"ERROR: {str(e)}")
    print("Fallback: Appending all data without deduplication")
    # Fallback approach - just append the data
    if os.path.exists(master_csv):
        df.to_csv(master_csv, mode='a', header=False, index=False)
    else:
        df.to_csv(master_csv, index=False)

print(f"Data collection process completed at {collection_time}")
