import argparse
import logging
import logging.config
import re
import time
from functools import partial

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine, types


def initialize_logs():

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


def check_rate_limit(result):
    second_rate, minute_rate = result.headers["X-App-Rate-Limit-Count"].split(",")
    second_rate_count = second_rate.split(":")[0]
    minute_rate_count = minute_rate.split(":")[0]

    if int(second_rate_count) >= 20:
        LOGGER.info("Wating 1 second because of rate limit (20 per sec)...")
        time.sleep(1)

    if int(minute_rate_count) >= 100:
        LOGGER.info("Wating for 2 minutes because rate limit (100 per 2min)...")
        time.sleep(120)


def get_match_id_list_for_summoner(puuid, count, end_timestamp=None):

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


def get_summoner_history(puuid, count, end_timestamp=None):

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

    # LOGGER.info(f"Received {len(matchid_list)} responses")

    return summoner_history


def insert_on_conflict_do_nothing(table, conn, keys, data_iter, conflict):

    columns = ", ".join(keys)
    sql = f"INSERT INTO {table.name}({columns}) VALUES %s ON CONFLICT ({conflict}) DO NOTHING"

    cursor = conn.connection.cursor()
    psycopg2.extras.execute_values(cursor, sql, list(data_iter))
    cursor.close()


def insert_datamodel(summoner_history):

    df = summoner_history.copy()

    df.columns = ["_".join(re.findall("[a-zA-Z][^A-Z]*", colname)).lower() for colname in df]
    df["team"] = df.team_id.replace({100: "Blue", 200: "Red"})

    match = df.filter(["match_id", "start_date", "queue_id"]).drop_duplicates()
    summoner = df.filter(["puuid", "summoner_name"]).drop_duplicates()
    champion = df.filter(["champion_id", "champion_name"]).drop_duplicates()
    summoner_history = df.filter(["batch_id", "match_id", "puuid", "champion_id", "team_position", "team", "win"])

    LOGGER.info("Start data insertion in PostgreSQL database 'riot'...")

    summoner.to_sql(
        name="summoner",
        con=ENGINE,
        if_exists="append",
        index=False,
        method=partial(insert_on_conflict_do_nothing, conflict="puuid"),
        dtype={"puuid": types.VARCHAR(100), "summoner_name": types.VARCHAR(20)},
    )

    champion.to_sql(
        name="champion",
        con=ENGINE,
        if_exists="append",
        index=False,
        method=partial(insert_on_conflict_do_nothing, conflict="champion_name"),
        dtype={
            "champion_id": types.BIGINT,
            "champion_name": types.VARCHAR(20),
            "has_enemies_allies_history": types.BOOLEAN,
        },
    )

    match.to_sql(
        name="match",
        con=ENGINE,
        if_exists="append",
        index=False,
        method=partial(insert_on_conflict_do_nothing, conflict="match_id"),
        dtype={"match_id": types.VARCHAR(30), "start_date": types.TIMESTAMP, "queue_id": types.NUMERIC(10, 1)},
    )

    try:
        summoner_history.to_sql(
            name="summoner_history",
            con=ENGINE,
            if_exists="append",
            index=False,
            method=partial(insert_on_conflict_do_nothing, conflict="match_id, puuid"),
            dtype={
                "puuid": types.VARCHAR(100),
                "summoner_name": types.VARCHAR(20),
                "champion_id": types.BIGINT,
                "team_position": types.VARCHAR(10),
                "team": types.VARCHAR(4),
                "win": types.BOOLEAN,
            },
        )
    except Exception:
        print(champion)
        print("\n")
        print(summoner_history)
        raise

    LOGGER.info("End of data insertion")


def select_summoner(default_summoner_name="XkabutoX"):

    sql_query = "select puuid from summoner where has_enemies_allies_history is false order by random() limit 1"

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


def main(history_depth=5):
    """
    First, get the match history of the analysed summoner (choosed randomly)
    Then look for the last 5 games of every players encoutered in these games
    """
    puuid = select_summoner()

    LOGGER.info(
        f"Try to send a maximun of {1 + history_depth + history_depth * (history_depth + 1) * 9} requests to riot api..."
    )

    summoner_history = get_summoner_history(puuid, history_depth)  # count + 1
    enemies_allies_encountered = summoner_history[["puuid", "startDate"]].query("puuid != @puuid").drop_duplicates()

    enemies_allies_history = [
        get_summoner_history(puuid, history_depth, int(end_timestamp.timestamp()))
        for puuid, end_timestamp in enemies_allies_encountered.values
    ]  # count * (count + 1) * 9

    LOGGER.info("Received all responses")
    all_history = pd.concat([summoner_history, *enemies_allies_history])
    all_history["batch_id"] = int(pd.Timestamp("today").timestamp())

    LOGGER.info(f"Update summoner table for {puuid}")
    sql_update = f"update summoner set has_enemies_allies_history = true where puuid = '{puuid}'"
    ENGINE.execute(sql_update)

    insert_datamodel(all_history)


if __name__ == "__main__":

    kwargs = initialize_kwargs()
    LOGGER = initialize_logs()
    ENGINE = create_engine(f"postgresql://postgres:{kwargs.DB_PASS}@localhost:5432/riot", encoding="utf8")

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
