import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List
from analyse_fpl.model import FPLAnalysisResponse, PlayerGameweekStats


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


def load_players_data(data_dir, gameweek_ids):
    """
    Load players.csv to get position and team_code data. Uses the most recent gameweek available.
    
    Args:
        data_dir: Base data directory (e.g., path to FPL-Core-Insights/data/2025-2026)
        gameweek_ids: List of gameweek IDs to try loading from
    
    Returns:
        pandas DataFrame with player position and team_code data, or empty DataFrame if not found
    """
    # Try to load from the most recent gameweek first
    for gw_id in sorted(gameweek_ids, reverse=True):
        players_path = Path(data_dir) / "By Gameweek" / f"GW{gw_id}" / "players.csv"
        if players_path.exists():
            players_df = pd.read_csv(players_path)
            logging.info(f"Loaded players data from GW{gw_id}")
            return players_df
    
    logging.warning("players.csv not found in any of the specified gameweeks")
    return pd.DataFrame()


def load_teams_data(data_dir, gameweek_ids):
    """
    Load teams.csv to get team information. Uses the most recent gameweek available.
    
    Args:
        data_dir: Base data directory (e.g., path to FPL-Core-Insights/data/2025-2026)
        gameweek_ids: List of gameweek IDs to try loading from
    
    Returns:
        pandas DataFrame with team data, or empty DataFrame if not found
    """
    # Try to load from the most recent gameweek first
    for gw_id in sorted(gameweek_ids, reverse=True):
        teams_path = Path(data_dir) / "By Gameweek" / f"GW{gw_id}" / "teams.csv"
        if teams_path.exists():
            teams_df = pd.read_csv(teams_path)
            logging.info(f"Loaded teams data from GW{gw_id}")
            return teams_df
    
    logging.warning("teams.csv not found in any of the specified gameweeks")
    return pd.DataFrame()


def load_player_gameweek_stats(data_dir, gameweek_ids, season=None):
    """
    Load player_gameweek_stats from multiple gameweeks and combine into a single dataframe.
    Also merges position data from players.csv.
    
    Args:
        data_dir: Base data directory (e.g., path to FPL-Core-Insights/data/2025-2026)
        gameweek_ids: List of gameweek IDs to load
        season: Season identifier (e.g., "2025-2026"). If None, extracted from data_dir path.
    
    Returns:
        Combined pandas DataFrame with all player gameweek stats, including position data
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
            logging.info(f"Loaded {len(df)} rows from GW{gw_id}")
        else:
            logging.warning(f"File not found for GW{gw_id}: {gw_path}")
    
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Load and merge position data from players.csv
        logging.info("Loading players position data...")
        players_df = load_players_data(data_dir, gameweek_ids)
        
        if not players_df.empty:
            # Merge position and team_code data using id (from player_gameweek_stats) and player_id (from players.csv)
            combined_df = combined_df.merge(
                players_df[['player_id', 'position', 'team_code']],
                left_on='id',
                right_on='player_id',
                how='left'
            )
            # Drop the redundant player_id column after merge
            combined_df = combined_df.drop(columns=['player_id'], errors='ignore')
            logging.info(f"Merged position and team_code data. Shape after merge: {combined_df.shape}")
        else:
            logging.warning("Could not load position data. Continuing without position column.")
        
        # Load and merge team data from teams.csv
        logging.info("Loading teams data...")
        teams_df = load_teams_data(data_dir, gameweek_ids)
        
        if not teams_df.empty and 'team_code' in combined_df.columns:
            # Select only the required team fields: name, strength-related fields, and elo
            strength_fields = [
                'strength', 'strength_overall_home', 'strength_overall_away',
                'strength_attack_home', 'strength_attack_away',
                'strength_defence_home', 'strength_defence_away'
            ]
            # Get available strength fields (in case some don't exist)
            available_strength_fields = [col for col in strength_fields if col in teams_df.columns]
            team_fields = ['name', 'elo'] + available_strength_fields
            
            # Merge team data using team_code (from players.csv) and code (from teams.csv)
            combined_df = combined_df.merge(
                teams_df[['code'] + team_fields],
                left_on='team_code',
                right_on='code',
                how='left',
                suffixes=('', '_team')
            )
            # Drop the redundant code column after merge
            combined_df = combined_df.drop(columns=['code'], errors='ignore')
            # Rename team-related fields with 'team_' prefix to distinguish them
            rename_dict = {}
            if 'name' in combined_df.columns:
                rename_dict['name'] = 'team_name'
            if 'elo' in combined_df.columns:
                rename_dict['elo'] = 'team_elo'
            # Rename all strength fields
            for field in strength_fields:
                if field in combined_df.columns:
                    rename_dict[field] = f'team_{field}'
            if rename_dict:
                combined_df = combined_df.rename(columns=rename_dict)
            logging.info(f"Merged team data. Shape after merge: {combined_df.shape}")
        else:
            if 'team_code' not in combined_df.columns:
                logging.warning("team_code not available. Cannot merge team data.")
            else:
                logging.warning("Could not load team data. Continuing without team columns.")
        
        # Filter out players with status 'u' (unavailable)
        initial_count = len(combined_df)
        combined_df = combined_df[combined_df['status'] != 'u'].copy()
        filtered_count = len(combined_df)
        logging.info(f"Filtered out {initial_count - filtered_count} rows with status 'u' (unavailable)")
        
        return combined_df
    else:
        return pd.DataFrame()


def get_next_gameweek(gameweek_summaries_path):
    """
    Get the next gameweek ID from gameweek summaries.
    
    Args:
        gameweek_summaries_path: Path to gameweek_summaries.csv
    
    Returns:
        Next gameweek ID (int) or None if not found
    """
    df = pd.read_csv(gameweek_summaries_path)
    
    # Convert boolean columns
    df['finished'] = df['finished'].astype(str).str.lower() == 'true'
    df['is_next'] = df['is_next'].astype(str).str.lower() == 'true'
    
    # Find the gameweek marked as next
    next_gw = df[df['is_next'] == True]
    if not next_gw.empty:
        return int(next_gw.iloc[0]['id'])
    
    # If no gameweek is marked as next, find the first unfinished gameweek
    unfinished = df[df['finished'] == False].copy()
    if not unfinished.empty:
        unfinished['deadline_time'] = pd.to_datetime(unfinished['deadline_time'])
        unfinished = unfinished.sort_values('deadline_time', ascending=True)
        return int(unfinished.iloc[0]['id'])
    
    return None


def analyse_fpl() -> FPLAnalysisResponse:
    # Paths - adjust these if needed
    fpl_data_dir = Path("../FPL-Core-Insights/data/2025-2026")
    gameweek_summaries_path = fpl_data_dir / "gameweek_summaries.csv"
    
    logging.info("Loading gameweek summaries...")
    recent_gameweeks = get_recent_finished_gameweeks(gameweek_summaries_path, num_gameweeks=5)
    
    if not recent_gameweeks:
        logging.warning("No finished and data_checked gameweeks found!")
        # Return empty response
        return FPLAnalysisResponse(
            past_gameweeks=[],
            next_gameweek=None,
            player_stats={}
        )
    
    logging.info(f"Most recent 5 finished gameweeks: {recent_gameweeks}")
    
    # Get next gameweek
    next_gameweek = get_next_gameweek(gameweek_summaries_path)
    logging.info(f"Next gameweek: {next_gameweek}")
    
    logging.info("Loading player gameweek stats...")
    player_stats_df = load_player_gameweek_stats(fpl_data_dir, recent_gameweeks)
    
    if player_stats_df.empty:
        logging.warning("No player stats data loaded.")
        return FPLAnalysisResponse(
            past_gameweeks=recent_gameweeks,
            next_gameweek=next_gameweek,
            player_stats={}
        )
    
    logging.info(f"Combined dataframe shape: {player_stats_df.shape}")
    
    # Transform dataframe into the required format
    # Group by player-position and convert rows to PlayerGameweekStats
    player_stats_dict: Dict[str, List[PlayerGameweekStats]] = {}
    
    # Ensure position column exists
    if 'position' not in player_stats_df.columns:
        logging.warning("Position column not found. Cannot create player-position keys.")
        return FPLAnalysisResponse(
            past_gameweeks=recent_gameweeks,
            next_gameweek=next_gameweek,
            player_stats={}
        )
    
    # Group by player id and position, then convert each group to list of PlayerGameweekStats
    for (player_id, position), group in player_stats_df.groupby(['id', 'position']):
        # Create key: "player_id-position"
        key = f"{player_id}-{position}"
        
        # Convert each row in the group to PlayerGameweekStats
        stats_list = []
        for _, row in group.iterrows():
            # Convert row to dict, handling NaN values
            row_dict = row.to_dict()
            # Remove NaN values (Pydantic doesn't like them)
            row_dict = {k: v for k, v in row_dict.items() if pd.notna(v)}
            try:
                stats = PlayerGameweekStats(**row_dict)
                stats_list.append(stats)
            except Exception as e:
                logging.warning(f"Error creating PlayerGameweekStats for player {player_id}, gameweek {row.get('gameweek')}: {e}")
                continue
        
        if stats_list:
            player_stats_dict[key] = stats_list
    
    logging.info(f"Created {len(player_stats_dict)} player-position entries")
    
    return FPLAnalysisResponse(
        past_gameweeks=recent_gameweeks,
        next_gameweek=next_gameweek,
        player_stats=player_stats_dict
    )


# if __name__ == "__main__":
#     print(analyse_fpl().model_dump_json())