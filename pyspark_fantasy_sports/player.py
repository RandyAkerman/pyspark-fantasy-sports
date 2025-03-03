from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType

class PlayerDataSource(DataSource):
    """
    A custom data source for reading player data from the Sleeper API.

    This data source reads player details such as IDs, names, team information, and other
    attributes from the Sleeper API and provides them as a Spark DataFrame.

    Methods:
        name: Returns the name of the data source.
        schema: Defines the schema for the player data source.
        reader: Creates a reader for the player data source.
    """

    @classmethod
    def name(cls) -> str:
        """
        Returns the name of the data source.
        """
        return "player"

    def schema(self):
        """
        Defines the schema for the player data source.

        Returns:
            StructType: The schema of the player data source.
        """
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
        return schema

    def reader(self, schema: StructType):
        """
        Creates a reader for the player data source.

        Args:
            schema (StructType): The schema of the player data source.

        Returns:
            PlayerDataSourceReader: The reader for the player data source.
        """
        return PlayerDataSourceReader(schema, self.options)
    
class PlayerDataSourceReader(DataSourceReader):
    """
    A reader for the player data source.
    """

    def __init__(self, schema, options):
        """
        Initializes the reader with the given schema and options.

        Args:
            schema (StructType): The schema of the player data source.
            options (dict): The options for the player data source.
        """
        self.schema: StructType = schema
        self.options = options

    def read(self, partition):
        """
        Reads data for a given partition.

        Args:
            partition: The partition to read data for.

        Yields:
            Row: A row of data for the given partition.
        """
        # Library imports must be within the method.
        import requests
        from pyspark.sql import Row
        # TODO: Add a league option, nfl, nba, etc.
        resp = requests.get("https://api.sleeper.app/v1/players/nfl")
        resp.raise_for_status()
        data = resp.json()
        for player in data.keys():
            yield Row(**{key: data[player].get(key) for key in [
                'sportradar_id', 'rotoworld_id', 'gsis_id', 'depth_chart_order', 'full_name', 'college',
                'search_first_name', 'injury_status', 'metadata', 'active', 'birth_country', 'fantasy_data_id',
                'injury_start_date', 'weight', 'competitions', 'rotowire_id', 'team', 'height', 'player_id',
                'depth_chart_position', 'last_name', 'opta_id', 'status', 'high_school', 'pandascore_id',
                'birth_city', 'fantasy_positions', 'yahoo_id', 'birth_state', 'sport', 'hashtag', 'age',
                'news_updated', 'team_abbr', 'swish_id', 'practice_description', 'years_exp', 'team_changed_at',
                'oddsjam_id', 'birth_date', 'injury_notes', 'number', 'injury_body_part', 'espn_id', 'position',
                'practice_participation', 'first_name', 'search_full_name', 'stats_id', 'search_rank', 'search_last_name'
            ]})

