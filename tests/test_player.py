import pytest
from pyspark_fantasy_sports import PlayerDataSource
from pyspark.sql.types import StructType, StructField, StringType

def test_read(mock_player_nfl_response):
    """
    Test the `read` method of the `PlayerDataSource` class.
    This test verifies that the `read` method correctly reads player data and returns the expected number of rows.
    Args:
        mock_player_nfl_response (Mock): A mock response object for player NFL data.
    Steps:
    1. Define the schema for the player data using `StructType` and `StructField`.
    2. Initialize the `PlayerDataSource` with the given options.
    3. Create a reader object using the `reader` method of `PlayerDataSource`.
    4. Read the player data using the reader and convert the result to a list of rows.
    5. Assert that the number of rows read is equal to 10.
    """
    # Arrange
    schema = StructType([
        StructField("sportradar_id", StringType(), True),
        StructField("rotoworld_id", StringType(), True),
        StructField("gsis_id", StringType(), True),
        StructField("depth_chart_order", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("college", StringType(), True),
        StructField("search_first_name", StringType(), True),
        StructField("injury_status", StringType(), True),
        StructField("metadata", StringType(), True),
        StructField("active", StringType(), True),
        StructField("birth_country", StringType(), True),
        StructField("fantasy_data_id", StringType(), True),
        StructField("injury_start_date", StringType(), True),
        StructField("weight", StringType(), True),
        StructField("competitions", StringType(), True),
        StructField("rotowire_id", StringType(), True),
        StructField("team", StringType(), True),
        StructField("height", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("depth_chart_position", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("opta_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("high_school", StringType(), True),
        StructField("pandascore_id", StringType(), True),
        StructField("birth_city", StringType(), True),
        StructField("fantasy_positions", StringType(), True),
        StructField("yahoo_id", StringType(), True),
        StructField("birth_state", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("hashtag", StringType(), True),
        StructField("age", StringType(), True),
        StructField("news_updated", StringType(), True),
        StructField("team_abbr", StringType(), True),
        StructField("swish_id", StringType(), True),
        StructField("practice_description", StringType(), True),
        StructField("years_exp", StringType(), True),
        StructField("team_changed_at", StringType(), True),
        StructField("oddsjam_id", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("injury_notes", StringType(), True),
        StructField("number", StringType(), True),
        StructField("injury_body_part", StringType(), True),
        StructField("espn_id", StringType(), True),
        StructField("position", StringType(), True),
        StructField("practice_participation", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("search_full_name", StringType(), True),
        StructField("stats_id", StringType(), True),
        StructField("search_rank", StringType(), True),
        StructField("search_last_name", StringType(), True),
    ])
    options = None
    player_data_source = PlayerDataSource(options)
    reader = player_data_source.reader(schema)
    
    # Act
    rows = list(reader.read(None))

    # Assert
    assert len(rows) == 10
