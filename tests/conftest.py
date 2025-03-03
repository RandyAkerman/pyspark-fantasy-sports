import pytest
from pathlib import Path
import json
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("pyspark-fantasy-sports-tests") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def player_nfl_path():
    return Path(__file__).parent / 'test_data' / 'player_nfl.json'

@pytest.fixture
def mock_player_nfl_response(mocker, player_nfl_path):
    with open(player_nfl_path) as f:
        player_nfl_data = json.load(f)
    
    mock = mocker.patch('requests.get')
    mock.return_value.status_code = 200
    mock.return_value.json.return_value = player_nfl_data
    return mock

@pytest.fixture
def league_nfl_path():
    return Path(__file__).parent / 'test_data' / 'league_nfl.json'

@pytest.fixture
def mock_league_nfl_response(mocker, league_nfl_path):
    with open(league_nfl_path) as f:
        league_nfl_data = json.load(f)
    
    mock = mocker.patch('requests.get')
    mock.return_value.status_code = 200
    mock.return_value.json.return_value = league_nfl_data
    return mock

@pytest.fixture
def league_options():
    return {"league_id": os.getenv('LEAGUE_ID')}