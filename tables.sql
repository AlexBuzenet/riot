DROP TABLE summoner_history,
match,
summoner,
champion,
batch;
CREATE TABLE batch (
    batch_id BIGINT,
    summoner_analysed VARCHAR(100) NOT NULL,
    PRIMARY KEY(batch_id, summoner_analysed)
);
CREATE TABLE champion (
    champion_id BIGINT PRIMARY KEY,
    champion_name VARCHAR(20) NOT NULL,
    UNIQUE(champion_name)
);
CREATE TABLE summoner (
    puuid VARCHAR(100) PRIMARY KEY,
    summoner_name VARCHAR(20) NOT NULL,
    is_summoner_analysed BOOLEAN DEFAULT FALSE,
    UNIQUE(puuid, summoner_name)
);
CREATE TABLE match (
    match_id VARCHAR(30) PRIMARY KEY,
    start_date TIMESTAMP NOT NULL,
    queue_id NUMERIC(10, 1) NOT NULL
);
CREATE TABLE summoner_history (
    batch_id INT NOT NULL,
    match_id VARCHAR(30) NOT NULL REFERENCES match(match_id),
    puuid VARCHAR(100) NOT NULL REFERENCES summoner(puuid),
    champion_id BIGINT NOT NULL REFERENCES champion(champion_id),
    team_position VARCHAR(10) NOT NULL,
    team VARCHAR(4) NOT NULL,
    win BOOLEAN NOT NULL,
    PRIMARY KEY(match_id, puuid)
);