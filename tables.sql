DROP TABLE summoner_history,
match,
summoner,
champion;
CREATE TABLE champion (
    champion_id BIGINT NOT NULL PRIMARY KEY,
    champion_name VARCHAR(20) NOT NULL,
    UNIQUE(champion_name)
);
-- INSERT INTO champion
-- VALUES (37225015, 'Unknown');
-- VALUES (37290552, 'Unknown2');
CREATE TABLE summoner (
    puuid VARCHAR(100) NOT NULL PRIMARY KEY,
    summoner_name VARCHAR(20) NOT NULL,
    has_enemies_allies_history BOOLEAN DEFAULT FALSE,
    UNIQUE(puuid, summoner_name)
);
CREATE TABLE match (
    match_id VARCHAR(30) NOT NULL PRIMARY KEY,
    start_date TIMESTAMP NOT NULL,
    queue_id NUMERIC(10, 1) NOT NULL
);
CREATE TABLE summoner_history (
    batch_id INT NOT NULL,
    match_id VARCHAR(30) REFERENCES match(match_id),
    puuid VARCHAR(100) REFERENCES summoner(puuid),
    champion_id BIGINT REFERENCES champion(champion_id),
    team_position VARCHAR(10) NOT NULL,
    team VARCHAR(4) NOT NULL,
    win BOOLEAN NOT NULL,
    PRIMARY KEY(match_id, puuid)
);