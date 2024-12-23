import polars as pl
import os

# Project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Input filenames
player_files = [os.path.join(project_root, 'data', f"skaters_{year}.csv") for year in range(2023, 2025)]
team_files = [os.path.join(project_root, 'data', f"teams_{year}.csv") for year in range(2023, 2025)]

# Output filename
skater_output_filename = os.path.join(project_root, 'skaters_combined_data.csv')
team_output_filename = os.path.join(project_root, 'teams_combined_data.csv')

# Read player DataFrames
player_dfs = []
for file in player_files:
    df = pl.read_csv(file)
    year = int(os.path.basename(file).split('_')[1].split('.')[0])
    df = df.with_columns(pl.Series(name="year", values=[year]))
    player_dfs.append(df)

# Read team DataFrames
team_dfs = []
for file in team_files:
    df = pl.read_csv(file)
    year = int(os.path.basename(file).split('_')[1].split('.')[0])
    df = df.with_columns(pl.Series(name="year", values=[year]))
    team_dfs.append(df)

try:
    # Attempt vertical concatenation
    combined_df = pl.concat(player_dfs + team_dfs)
    
    print(f"Successfully concatenated DataFrames. Shape: {combined_df.shape}")
    
    # Save to CSV
    combined_df.write_csv(output_filename)
    print(f"Combined data saved to {output_filename}")

except Exception as e:
    print(f"Concatenation failed: {str(e)}")
    print("Check column alignment or DataFrame structures.")
