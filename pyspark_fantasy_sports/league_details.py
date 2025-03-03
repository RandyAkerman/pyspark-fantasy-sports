from dataclasses import dataclass

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType

# TODO: change this to a list of league IDs
class LeagueDetailsDataSource(DataSource):
    """
    A custom data source for reading league details data from the Sleeper API. This data source reads league details such as name, status, metadata, settings, and other
    attributes from the Sleeper API and provides them as a Spark DataFrame.

    Options
    -------
    
    - league_id: a string of a single league ID to read data for.

    Examples
    --------
    Register the data source.

    >>> from pyspark_fantasy_sports import LeagueDetailsDataSource
    >>> spark.dataSource.register(LeagueDetailsDataSource)


    Define the options for the custom data source
    
    >>> options = {
    ...    "league_id": "12345",
    ... }

    Create a DataFrame using the custom data source
    >>> league_df = spark.readStream.format("league_details").options(**options).load()

    """

    @classmethod
    def name(cls) -> str:
        """
        Returns the name of the data source.
        """
        return "league_details"

    def schema(self):
        """
        Defines the schema for the league details data source.

        Returns:
            StructType: The schema of the league details data source.
        """
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
        return schema

    def reader(self, schema: StructType):
        """
        Creates a reader for the league details data source.

        Args:
            schema (StructType): The schema of the league details data source.

        Returns:
            LeagueDetailsDataSourceReader: The reader for the league details data source.
        """
        return LeagueDetailsDataSourceReader(schema, self.options)

class LeagueDetailsDataSourceReader(DataSourceReader):
    """
    A reader for the league details data source.
    """

    def __init__(self, schema, options: dict):
        """
        Initializes the reader with the given schema and options.

        Args:
            schema (StructType): The schema of the league details data source.
            options (dict): The options for the league details data source.
        """
        # super().__init__()
        self.schema: StructType = schema
        self.options = options
        self.league_id = options.get("league_id","")

    def read(self,partition):
        """
        Reads data for a given partition from the Sleeper API and yields it as a PySpark Row.

        Args:
            partition: partition seems to be required but not used.

        Yields:
            Row: A row of data containing league details fetched from the Sleeper API.
        """
        # Library imports must be within the method.
        import requests
        from pyspark.sql import Row

        league = self.league_id
        
        resp = requests.get(f"https://api.sleeper.app/v1/league/{league}")
        resp.raise_for_status()
        data = resp.json()
        
        yield Row(**data)


