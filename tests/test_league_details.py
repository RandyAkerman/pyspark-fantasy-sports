from numpy import partition
import pytest
from pyspark_fantasy_sports import LeagueDetailsDataSource
from pyspark.sql.types import StructType, StructField, StringType

# TODO: Look up good test for API calls to check for what happens during failures 400 codes, etc.
# TODO: Test for multiple league IDs

def test_read(mock_league_nfl_response,league_options):
    """
    Test the `read` method of the `LeagueDetailsDataSource` class.
    This test verifies that the `read` method correctly reads data from the data source
    and returns the expected number of rows.
    Args:
        mock_league_nfl_response (Mock): A mock response object for the league NFL data.
    Steps:
    1. Define the schema for the league data using `StructType` and `StructField`.
    2. Set the options for the data source, including the path from the environment variable `LEAGUE_ID`.
    3. Create an instance of `LeagueDetailsDataSource` with the specified options.
    4. Get the reader for the data source using the defined schema.
    5. Retrieve the partitions from the reader.
    6. Read the data from each partition and collect the rows.
    7. Assert that the number of rows read is equal to 1.
    """
    # Arrange
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("metadata", StringType(), True),
        StructField("settings", StringType(), True),
        StructField("avatar", StringType(), True),
        StructField("company_id", StringType(), True),
        StructField("last_message_id", StringType(), True),
        StructField("shard", StringType(), True),
        StructField("season", StringType(), True),
        StructField("season_type", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("scoring_settings", StringType(), True),
        StructField("last_author_avatar", StringType(), True),    
        StructField("last_author_display_name", StringType(), True),
        StructField("last_author_id", StringType(), True),
        StructField("last_author_is_bot", StringType(), True),
        StructField("last_message_attachment", StringType(), True),
        StructField("last_message_text_map", StringType(), True),
        StructField("last_message_time", StringType(), True),
        StructField("last_pinned_message_id", StringType(), True),
        StructField("draft_id", StringType(), True),
        StructField("last_read_id", StringType(), True),
        StructField("league_id", StringType(), True),
        StructField("previous_league_id", StringType(), True),
        StructField("roster_positions", StringType(), True),
        StructField("bracket_id", StringType(), True),
        StructField("bracket_overrides_id", StringType(), True),
        StructField("group_id", StringType(), True),
        StructField("loser_bracket_id", StringType(), True),
        StructField("loser_bracket_overrides_id", StringType(), True),
        StructField("total_rosters", StringType(), True), 
    ])
    
    league_data_source = LeagueDetailsDataSource(league_options)
    reader = league_data_source.reader(schema)

    # Act
    rows = [row for row in reader.read(reader.league_id)]

    # Assert
    assert len(rows) == 1
 