import pandas as pd
from pathlib import Path


def get_recent_finished_gameweeks(gameweek_summaries_path, num_gameweeks=5):
    """
    Read gameweek summaries and return the most recent N finished and data_checked gameweeks.
    
    Args:
        gameweek_summaries_path: Path to gameweek_summaries.csv
        num_gameweeks: Number of recent gameweeks to return (default: 5)
    
    Returns:
        List of gameweek IDs (integers) sorted by most recent first
    """
    df = pd.read_csv(gameweek_summaries_path)
    
    # Convert boolean columns - handle both string "True"/"False" and actual booleans
    df['finished'] = df['finished'].astype(str).str.lower() == 'true'
    df['data_checked'] = df['data_checked'].astype(str).str.lower() == 'true'
    
    # Filter for finished and data_checked gameweeks
    finished_checked = df[(df['finished'] == True) & (df['data_checked'] == True)].copy()
    
    # Sort by deadline_time (most recent first)
    # Convert deadline_time to datetime for proper sorting
    finished_checked['deadline_time'] = pd.to_datetime(finished_checked['deadline_time'])
    finished_checked = finished_checked.sort_values('deadline_time', ascending=False)
    
    # Get the top N gameweek IDs
    recent_gameweeks = finished_checked.head(num_gameweeks)['id'].tolist()
    
    return recent_gameweeks


def load_player_gameweek_stats(data_dir, gameweek_ids, season=None):
    """
    Load player_gameweek_stats from multiple gameweeks and combine into a single dataframe.
    
    Args:
        data_dir: Base data directory (e.g., path to FPL-Core-Insights/data/2025-2026)
        gameweek_ids: List of gameweek IDs to load
        season: Season identifier (e.g., "2025-2026"). If None, extracted from data_dir path.
    
    Returns:
        Combined pandas DataFrame with all player gameweek stats
    """
    # Extract season from data_dir if not provided
    if season is None:
        season = Path(data_dir).name
    
    dataframes = []
    
    for gw_id in gameweek_ids:
        gw_path = Path(data_dir) / "By Gameweek" / f"GW{gw_id}" / "player_gameweek_stats.csv"
        
        if gw_path.exists():
            df = pd.read_csv(gw_path)
            # Add gameweek column to identify which gameweek this data belongs to
            df['gameweek'] = gw_id
            # Add season column to identify which season this data belongs to
            df['season'] = season
            dataframes.append(df)
            print(f"Loaded {len(df)} rows from GW{gw_id}")
        else:
            print(f"Warning: File not found for GW{gw_id}: {gw_path}")
    
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()


def main():
    # Paths - adjust these if needed
    fpl_data_dir = Path("/Users/wenhao.aw/Desktop/misc_projects/FPL-Core-Insights/data/2025-2026")
    gameweek_summaries_path = fpl_data_dir / "gameweek_summaries.csv"
    
    print("Loading gameweek summaries...")
    recent_gameweeks = get_recent_finished_gameweeks(gameweek_summaries_path, num_gameweeks=5)
    
    if not recent_gameweeks:
        print("No finished and data_checked gameweeks found!")
        return
    
    print(f"\nMost recent 5 finished gameweeks: {recent_gameweeks}")
    
    print("\nLoading player gameweek stats...")
    player_stats_df = load_player_gameweek_stats(fpl_data_dir, recent_gameweeks)
    
    if not player_stats_df.empty:
        print(f"\nCombined dataframe shape: {player_stats_df.shape}")
        print(f"Columns: {list(player_stats_df.columns)}")
        print(f"\nFirst few rows:")
        print(player_stats_df.head())
    else:
        print("\nNo player stats data loaded.")


if __name__ == "__main__":
    main()
