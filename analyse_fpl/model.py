from typing import Dict, List, Optional, Any
from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class PlayerGameweekStats(BaseModel):
    """Represents a single row of player gameweek statistics from the dataframe."""
    
    # Player identification
    id: int
    first_name: Optional[str] = None
    second_name: Optional[str] = None
    web_name: Optional[str] = None
    position: Optional[str] = None
    
    # Gameweek and season
    gameweek: int
    season: str
    
    # Player status and info
    status: Optional[str] = None
    news: Optional[str] = None
    now_cost: Optional[float] = None
    
    # Performance stats
    total_points: Optional[int] = None
    minutes: Optional[int] = None
    goals_scored: Optional[int] = None
    assists: Optional[int] = None
    clean_sheets: Optional[int] = None
    goals_conceded: Optional[int] = None
    bonus: Optional[int] = None
    bps: Optional[int] = None
    
    # Team information
    team_code: Optional[int] = None
    team_name: Optional[str] = None
    team_elo: Optional[float] = None
    team_strength: Optional[int] = None
    team_strength_overall_home: Optional[int] = None
    team_strength_overall_away: Optional[int] = None
    team_strength_attack_home: Optional[int] = None
    team_strength_attack_away: Optional[int] = None
    team_strength_defence_home: Optional[int] = None
    team_strength_defence_away: Optional[int] = None
    
    # Additional fields - using Any for flexibility with remaining dataframe columns
    model_config = ConfigDict(extra="allow")  # Allow extra fields from the dataframe
    
    @field_validator('now_cost', 'team_elo', mode='before')
    @classmethod
    def round_floats(cls, v: Any) -> Any:
        """Round float values to 2 decimal places."""
        if v is not None and isinstance(v, (int, float)):
            return round(float(v), 2)
        return v
    
    @model_validator(mode='after')
    def round_extra_float_fields(self):
        """Round any extra float fields from the dataframe to 2 decimal places."""
        if hasattr(self, '__pydantic_extra__') and self.__pydantic_extra__:
            for key, value in self.__pydantic_extra__.items():
                if isinstance(value, (float)):
                    self.__pydantic_extra__[key] = round(float(value), 2)
        return self


class FPLAnalysisResponse(BaseModel):
    """Response model for FPL analysis containing past gameweeks and player stats."""
    
    past_gameweeks: List[int]
    next_gameweek: Optional[int] = None
    player_stats: Dict[str, List[PlayerGameweekStats]]
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "past_gameweeks": [22, 23, 24, 25, 26],
                "next_gameweek": 27,
                "player_stats": {
                    "123-Midfielder": [
                        {
                            "id": 123,
                            "first_name": "John",
                            "second_name": "Doe",
                            "gameweek": 25,
                            "total_points": 8,
                            "position": "Midfielder"
                        }
                    ]
                }
            }
        }
    )
