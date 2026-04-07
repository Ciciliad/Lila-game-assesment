import pandas as pd
import os
import json

# Map folders to output files
folder_to_file = {
    "February_10": "feb10.json",
    "February_11": "feb11.json",
    "February_12": "feb12.json",
    "February_13": "feb13.json",
    "February_14": "feb14.json"
}

for folder, output_file in folder_to_file.items():
    print(f"\nProcessing {folder}...")

    all_data = []
    folder_path = os.path.join(".", folder)

    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)

        try:
            df = pd.read_parquet(file_path)

            # Add date
            df['date'] = folder

            # Decode event column
            df['event'] = df['event'].apply(
                lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
            )

            # Add bot flag
            df['is_bot'] = df['user_id'].apply(lambda x: str(x).isdigit())

            # Convert timestamp to milliseconds
            df['ts'] = pd.to_datetime(df['ts']).astype('int64') // 10**6

            # Keep only required columns
            df = df[['user_id', 'match_id', 'map_id', 'x', 'z', 'ts', 'event', 'is_bot', 'date']]

            all_data.append(df)

        except Exception as e:
            print(f"Skipping file {file}: {e}")

    # Combine all files of the day
    final_df = pd.concat(all_data, ignore_index=True)

    # Sort for timeline playback
    final_df = final_df.sort_values(by=['match_id', 'ts'])

    # Group by match_id
    grouped = {}

    for match_id, group in final_df.groupby('match_id'):
        grouped[match_id] = group.to_dict(orient='records')

    # Save JSON
    with open(output_file, "w") as f:
        json.dump(grouped, f, indent=2)

    print(f"✅ {output_file} created successfully!")

print("\n🎉 All days converted successfully!")