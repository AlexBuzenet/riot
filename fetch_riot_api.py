import argparse
import logging
import logging.config
import re
import time
from functools import partial
from typing import Iterable, List

import pandas as pd
import psycopg2
import requests
import sqlalchemy
from sqlalchemy import create_engine, types


def initialize_logs() -> logging.Logger:

    log_path = "/mnt/c/Users/alexb/Desktop/Data science Python/riot/logs.log"

    logging.config.fileConfig(
        fname="/mnt/c/Users/alexb/Desktop/Data science Python/riot/logs.conf", defaults={"logfilename": log_path}
    )

    return logging.getLogger(__name__)


def initialize_kwargs() -> argparse.Namespace:

    parser = argparse.ArgumentParser(description="")

    parser.add_argument("--API_KEY", metavar="API_KEY", help="Riot api key", required=True)
    parser.add_argument("--DB_PASS", metavar="DB_PASS", help="Password for the database connection", required=True)

    return parser.parse_args()


def check_rate_limit(result: requests.models.Response) -> None:
    second_rate, minute_rate = result.headers["X-App-Rate-Limit-Count"].split(",")
    second_rate_count = second_rate.split(":")[0]
    minute_rate_count = minute_rate.split(":")[0]

    if int(second_rate_count) >= 20:
        LOGGER.info("Wating 1 second because of rate limit (20 per sec)...")
        time.sleep(1)

    if int(minute_rate_count) >= 100:
        LOGGER.info("Wating for 2 minutes because rate limit (100 per 2min)...")
        time.sleep(120)


def get_match_id_list_for_summoner(puuid: str, count: int, end_timestamp: pd.Timestamp = None):

    params_matchlist = {
        "api_key": kwargs.API_KEY,
        "type": "ranked",
        # "queue": 420,
        "start": 0,
        "count": count,
    }
    if end_timestamp:
        params_matchlist["endTime"] = end_timestamp

    result = requests.get(
        f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids",
        params=params_matchlist,
        timeout=100,
    )
    check_rate_limit(result)

    return result.json()


def get_summoner_history(puuid: str, count: int, end_timestamp: pd.Timestamp = None) -> pd.DataFrame:

    matchid_list = get_match_id_list_for_summoner(puuid, count, end_timestamp)

    def filter_dict(dict_, filters):
        return {key: value for key, value in dict_.items() if key in filters}

    participant_keys = [
        "puuid",
        # "participantId",
        # "summonerId",
        "summonerName",
        "championId",
        "championName",
        "teamPosition",
        # "individualPosition",
        # "lane",
        # "role",
        "teamId",
        "win",
    ]

    params_matches = {"api_key": kwargs.API_KEY}

    summoner_history = pd.DataFrame(columns=participant_keys)

    for matchid in matchid_list:

        match = requests.get(
            f"https://europe.api.riotgames.com/lol/match/v5/matches/{matchid}", params=params_matches, timeout=100
        )
        check_rate_limit(match)

        if not match.ok:
            LOGGER.error(match.text)
        match = match.json()

        participant_list = [
            filter_dict(participant, participant_keys) for participant in match["info"]["participants"]
        ]
        temp_summoner_history = pd.DataFrame(participant_list, columns=participant_keys)

        temp_summoner_history["startDate"] = pd.Timestamp(match["info"]["gameStartTimestamp"], unit="ms")
        temp_summoner_history["queueId"] = match["info"]["queueId"]
        temp_summoner_history["matchId"] = matchid

        summoner_history = pd.concat([summoner_history, temp_summoner_history])

    return summoner_history


def insert_on_conflict_do_nothing(
    table: pd.io.sql.SQLTable, conn: sqlalchemy.engine.Engine, keys: List[str], data_iter: Iterable, conflict: str
) -> None:

    columns = ", ".join(keys)
    sql = f"INSERT INTO {table.name}({columns}) VALUES %s ON CONFLICT ({conflict}) DO NOTHING"

    with conn.connection.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, list(data_iter))


def insert_datamodel(df: pd.DataFrame, puuid: str) -> None:

    df.columns = ["_".join(re.findall("[a-zA-Z][^A-Z]*", colname)).lower() for colname in df]

    df["team"] = df.team_id.replace({100: "Blue", 200: "Red"})
    df["batch_id"] = int(pd.Timestamp("today").timestamp())

    batch = (
        df.filter(["batch_id", "puuid"])
        .query(f"puuid=='{puuid}'")
        .rename(columns={"puuid": "summoner_analysed"})
        .drop_duplicates()
    )
    match = df.filter(["match_id", "start_date", "queue_id"]).drop_duplicates()
    summoner = df.filter(["puuid", "summoner_name"]).drop_duplicates()
    champion = df.filter(["champion_id", "champion_name"]).drop_duplicates()
    summoner_history = df.filter(["batch_id", "match_id", "puuid", "champion_id", "team_position", "team", "win"])

    LOGGER.info("Start data insertion in PostgreSQL database 'riot'...")

    with ENGINE.begin() as conn:

        batch.to_sql(
            name="batch",
            con=conn,
            if_exists="append",
            index=False,
            method=partial(insert_on_conflict_do_nothing, conflict="batch_id, summoner_analysed"),
            dtype={"batch_id": types.VARCHAR(100), "summoner_analysed": types.VARCHAR(20)},
        )

        summoner.to_sql(
            name="summoner",
            con=conn,
            if_exists="append",
            index=False,
            method=partial(insert_on_conflict_do_nothing, conflict="puuid"),
            dtype={"puuid": types.VARCHAR(100), "summoner_name": types.VARCHAR(20)},
        )

        champion.to_sql(
            name="champion",
            con=conn,
            if_exists="append",
            index=False,
            method=partial(insert_on_conflict_do_nothing, conflict="champion_name"),
            dtype={
                "champion_id": types.BIGINT,
                "champion_name": types.VARCHAR(20),
                "is_summoner_analysed": types.BOOLEAN,
            },
        )

        match.to_sql(
            name="match",
            con=conn,
            if_exists="append",
            index=False,
            method=partial(insert_on_conflict_do_nothing, conflict="match_id"),
            dtype={"match_id": types.VARCHAR(30), "start_date": types.TIMESTAMP, "queue_id": types.NUMERIC(10, 1)},
        )

        summoner_history.to_sql(
            name="summoner_history",
            con=conn,
            if_exists="append",
            index=False,
            method=partial(insert_on_conflict_do_nothing, conflict="match_id, puuid"),
            dtype={
                "batch_id": types.BIGINT,
                "puuid": types.VARCHAR(100),
                "summoner_name": types.VARCHAR(20),
                "champion_id": types.BIGINT,
                "team_position": types.VARCHAR(10),
                "team": types.VARCHAR(4),
                "win": types.BOOLEAN,
            },
        )

        LOGGER.info("End of data insertion")

        LOGGER.info(f"Update summoner table for {puuid}")
        conn.execute(f"update summoner set is_summoner_analysed = true where puuid = '{puuid}'")


def select_summoner(default_summoner_name: str = "XkabutoX") -> str:

    sql_query = "select puuid from summoner where is_summoner_analysed is false order by random() limit 1"

    result = pd.read_sql(sql_query, con=ENGINE)

    if result.empty:
        params_summoners = {"api_key": kwargs.API_KEY}
        result = requests.get(
            f"https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-name/{default_summoner_name}",
            params=params_summoners,
            timeout=3,
        )
        check_rate_limit(result)
        puuid = result.json()["puuid"]
        LOGGER.info(f"Use default summoner name {default_summoner_name} as a starting point")
    else:
        puuid = result.values[0][0]
        LOGGER.info(f"Analyse history of random summoner: puuid = {puuid}")

    return puuid


def main(history_depth: int = 5) -> None:
    """
    First, get the match history of the analysed summoner (choosen randomly or by default if first call)
    Then fetch history of every players encoutered in these games with depth = history_depth
    Save the data in a local postgres database
    """
    puuid_analysed = select_summoner()

    LOGGER.info(
        f"Sending a maximun of {1 + history_depth + history_depth * (history_depth + 1) * 9} requests to Riot API..."
    )

    summoner_history = get_summoner_history(puuid_analysed, history_depth)  # count + 1
    enemies_allies_encountered = (
        summoner_history[["puuid", "startDate"]].query("puuid != @puuid_analysed").drop_duplicates()
    )

    enemies_allies_history = [
        get_summoner_history(puuid, history_depth, int(end_timestamp.timestamp()))
        for puuid, end_timestamp in enemies_allies_encountered.values
    ]  # count * (count + 1) * 9

    LOGGER.info("Received all responses")
    all_history = pd.concat([summoner_history, *enemies_allies_history])

    LOGGER.info(f"Bulk batch contains {all_history.shape[0]} lines")

    insert_datamodel(all_history, puuid_analysed)


if __name__ == "__main__":

    kwargs = initialize_kwargs()
    LOGGER = initialize_logs()
    ENGINE = create_engine(f"postgresql+psycopg2://postgres:{kwargs.DB_PASS}@localhost:5432/riot", encoding="utf8")

    LOGGER.info("Program starting")

    try:
        main()
    except Exception as exception:
        LOGGER.error(exception)
        LOGGER.error("Program failed")
    else:
        LOGGER.info("Program ends successfully")
    finally:
        LOGGER.info("-" * 150)
        ENGINE.dispose()
