-- SQL Queries for Player Statistics

-- Query 1: Get unique players with their latest statistics
-- This query returns a table with unique players and their corresponding latest statistics
SELECT p.id, p.name, p.steam_id, p.team, ps.kills, ps.deaths, ps.assists, ps.kd_ratio, ps.damage_per_round, ps.damage
FROM players p
JOIN (
    SELECT player_name, MAX("second") AS latest_second
    FROM player_stats
    GROUP BY player_name
) latest ON p.name = latest.player_name
JOIN player_stats ps ON ps.player_name = latest.player_name AND ps."second" = latest.latest_second
ORDER BY p.id;

-- Query 2: Get damage per round over time for all players in the players table, grouped by player names
-- This query is designed for Grafana to plot damage_per_round trends over time for all players
-- The player_name field can be used in Grafana to group the data and display separate trend lines for each player
select *
from player_stats ps
         Join Players p on p.id = 1
where ps.player_name = p.name;
-- JOIN players p ON ps.player_name = p.name
-- ORDER BY ps.player_name, ps."second";

-- Query 3: Create a pivoted table for damage_per_round statistics with one column per player
-- This query creates a view where each player has their own column for damage_per_round statistics
-- The "second" field is included as an additional column as required
-- Using conditional aggregation to pivot the data
-- Ensuring every field has a value (0 if no value exists, latest value if it exists)
CREATE OR REPLACE VIEW player_damage_per_round_pivoted AS
WITH seconds AS (
    SELECT DISTINCT "second" FROM player_stats ORDER BY "second"
),
player_names AS (
    SELECT id, name FROM players ORDER BY id
),
player_stats_with_latest AS (
    SELECT 
        s."second",
        p.id AS player_id,
        p.name AS player_name,
        COALESCE(
            (SELECT ps.damage_per_round 
             FROM player_stats ps 
             WHERE ps.player_name = p.name AND ps."second" <= s."second" 
             ORDER BY ps."second" DESC 
             LIMIT 1),
            0
        ) AS latest_damage_per_round
    FROM seconds s
    CROSS JOIN player_names p
)
SELECT
    s."second",
    MAX(CASE WHEN p.id = 1 THEN pswl.latest_damage_per_round ELSE 0 END) AS player1,
    MAX(CASE WHEN p.id = 2 THEN pswl.latest_damage_per_round ELSE 0 END) AS player2,
    MAX(CASE WHEN p.id = 3 THEN pswl.latest_damage_per_round ELSE 0 END) AS player3,
    MAX(CASE WHEN p.id = 4 THEN pswl.latest_damage_per_round ELSE 0 END) AS player4,
    MAX(CASE WHEN p.id = 5 THEN pswl.latest_damage_per_round ELSE 0 END) AS player5,
    MAX(CASE WHEN p.id = 6 THEN pswl.latest_damage_per_round ELSE 0 END) AS player6,
    MAX(CASE WHEN p.id = 7 THEN pswl.latest_damage_per_round ELSE 0 END) AS player7,
    MAX(CASE WHEN p.id = 8 THEN pswl.latest_damage_per_round ELSE 0 END) AS player8,
    MAX(CASE WHEN p.id = 9 THEN pswl.latest_damage_per_round ELSE 0 END) AS player9,
    MAX(CASE WHEN p.id = 10 THEN pswl.latest_damage_per_round ELSE 0 END) AS player10
FROM seconds s
CROSS JOIN player_names p
LEFT JOIN player_stats_with_latest pswl ON s."second" = pswl."second" AND p.id = pswl.player_id
GROUP BY s."second"
ORDER BY s."second";

SELECT * FROM player_damage_per_round_pivoted;


DROP VIEW IF EXISTS player_damage_per_round_pivoted;
