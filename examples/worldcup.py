#!/usr/bin/env python3
"""FIFA World Cup semantic model — games, teams, and goals.

Data: Fjelstul World Cup Database, 30 tournaments (men's 1930-2022,
women's 1991-2019).

  Canonical source: https://github.com/jfjelstul/worldcup (CC-BY-SA 4.0)
  Also on Kaggle:   https://www.kaggle.com/datasets/joshfjelstul/world-cup-database

The parquet URLs below are a convenience mirror of the same cut. If you
republish anything derived from this data, attribute the Fjelstul World Cup
Database, not the mirror.

This example demonstrates:
  - A match-centric semantic model with joins to tournaments and stadiums
  - The team-match grain (team_appearances) for W/D/L and win-rate analysis
  - A goal-grain model for scorer analysis, including percent-of-total
  - A penalty-kick grain model for shootout conversion analysis
"""

import xorq.api as xo
from xorq.api import _

from boring_semantic_layer import entity_dimension, to_semantic_table

BASE_URL = "https://storage.googleapis.com/malloyyo/worldcup"


def canonicalize_team_name(team_name):
    """Roll historical team names into the successor used for analysis."""
    return (team_name == "West Germany").ifelse("Germany", team_name)


# --------------------------------------------------------------------
# Tournaments — one row per World Cup edition. The source of truth for
# year, host, winner, and men's vs women's.
# Lookup models used in fact joins intentionally contain dimensions only.
# Their standalone counterparts add measures at the entity's native grain,
# preventing counts and averages from being evaluated over repeated fact rows.
tournament_dimensions = to_semantic_table(
    xo.deferred_read_parquet(f"{BASE_URL}/tournaments.parquet"), name="tournaments"
).with_dimensions(
    tournament_id=entity_dimension(lambda t: t.tournament_id),
    tournament_name=_.tournament_name,
    year=_.year,
    decade=(_.year // 10) * 10,
    womens=_.tournament_name.contains("Women's"),
    host_country=_.host_country,
    winner=_.winner,
    start_date=_.start_date,
)
tournaments = tournament_dimensions.with_measures(
    tournament_count=_.count(),
    avg_teams=_.count_teams.mean(),
    host_win_count=_.host_won.sum(),
)

# --------------------------------------------------------------------
# Teams — national teams with confederation and region.
team_dimensions = to_semantic_table(
    xo.deferred_read_parquet(f"{BASE_URL}/teams.parquet"), name="teams"
).with_dimensions(
    team_id=entity_dimension(lambda t: t.team_id),
    team_name=_.team_name,
    canonical_team_name=canonicalize_team_name(_.team_name),
    team_code=_.team_code,
    confederation_name=_.confederation_name,
    confederation_code=_.confederation_code,
    region_name=_.region_name,
)
teams = team_dimensions.with_measures(
    team_count=_.count(),
)

# --------------------------------------------------------------------
# Stadiums — venues.
stadium_dimensions = to_semantic_table(
    xo.deferred_read_parquet(f"{BASE_URL}/stadiums.parquet"), name="stadiums"
).with_dimensions(
    stadium_id=entity_dimension(lambda t: t.stadium_id),
    stadium_name=_.stadium_name,
    city_name=_.city_name,
    country_name=_.country_name,
    stadium_capacity=_.stadium_capacity,
)
stadiums = stadium_dimensions.with_measures(
    stadium_count=_.count(),
    avg_capacity=_.stadium_capacity.mean(),
)

# --------------------------------------------------------------------
# Matches — one row per game. The central hub for match-level analysis.
# home/away scores exclude penalty shootouts (score_penalties has those).
matches = (
    to_semantic_table(xo.deferred_read_parquet(f"{BASE_URL}/matches.parquet"), name="matches")
    .with_dimensions(
        match_id=entity_dimension(lambda t: t.match_id),
        match_name=_.match_name,
        match_date=_.match_date,
        stage_name=_.stage_name,
        group_name=_.group_name,
        knockout_stage=_.knockout_stage,
        home_team_name=_.home_team_name,
        away_team_name=_.away_team_name,
        score=_.score,
        total_goals=_.home_team_score + _.away_team_score,
        result=_.result,
        extra_time=_.extra_time,
        penalty_shootout=_.penalty_shootout,
        city_name=_.city_name,
        country_name=_.country_name,
    )
    .with_measures(
        match_count=_.count(),
        goals_scored=(_.home_team_score + _.away_team_score).sum(),
        avg_goals_per_match=(_.home_team_score + _.away_team_score).mean(),
        draw_count=_.draw.sum(),
        draw_rate=_.draw.mean(),
        extra_time_count=_.extra_time.sum(),
        shootout_count=_.penalty_shootout.sum(),
    )
    .join_one(tournament_dimensions, on="tournament_id")
    .join_one(stadium_dimensions, on="stadium_id")
)

# --------------------------------------------------------------------
# Team appearances — one row per team per match (the team-match grain).
# The entry point for W/D/L records and win rates, without the
# home/away column gymnastics of the matches table.
team_appearances = (
    to_semantic_table(
        xo.deferred_read_parquet(f"{BASE_URL}/team_appearances.parquet"), name="team_appearances"
    )
    .with_dimensions(
        match_id=entity_dimension(lambda t: t.match_id),
        team_id=entity_dimension(lambda t: t.team_id),
        team_name=_.team_name,
        canonical_team_name=canonicalize_team_name(_.team_name),
        team_code=_.team_code,
        opponent_name=_.opponent_name,
        stage_name=_.stage_name,
        match_date=_.match_date,
        home_team=_.home_team,
        result=_.result,
    )
    .with_measures(
        game_count=_.count(),
        win_count=_.win.sum(),
        loss_count=_.lose.sum(),
        draw_count=_.draw.sum(),
        win_pct=_.win.mean(),
        goals_for_total=_.goals_for.sum(),
        goals_against_total=_.goals_against.sum(),
        goal_difference=_.goal_differential.sum(),
        avg_goals_for=_.goals_for.mean(),
        clean_sheet_count=(_.goals_against == 0).sum(),
    )
    .join_one(team_dimensions, on="team_id")
    .join_one(tournament_dimensions, on="tournament_id")
)

# --------------------------------------------------------------------
# Goals — one row per goal. team_name is the team the goal counts FOR
# (the opponent for own goals); player_team_name is the scorer's team.
# Single-named players (Pelé, Marta, ...) have given_name 'not applicable'.
goals = (
    to_semantic_table(xo.deferred_read_parquet(f"{BASE_URL}/goals.parquet"), name="goals")
    .with_dimensions(
        goal_id=entity_dimension(lambda t: t.goal_id),
        player_name=(_.given_name == "not applicable").ifelse(
            _.family_name, _.given_name + " " + _.family_name
        ),
        team_name=_.team_name,
        canonical_team_name=canonicalize_team_name(_.team_name),
        player_team_name=_.player_team_name,
        stage_name=_.stage_name,
        match_date=_.match_date,
        match_period=_.match_period,
        own_goal=_.own_goal,
        penalty=_.penalty,
    )
    .with_measures(
        goal_count=_.count(),
        penalty_count=_.penalty.sum(),
        own_goal_count=_.own_goal.sum(),
        avg_minute=_.minute_regulation.mean(),
        scorer_count=_.player_id.nunique(),
        # Percent-of-total: reference the declared measure by name;
        # t.all(...) computes the total across the whole query result.
        pct_of_goals=lambda t: t.goal_count.cast("float64") / t.all(t.goal_count) * 100,
    )
    .join_one(tournament_dimensions, on="tournament_id")
)

# --------------------------------------------------------------------
# Penalty kicks — one row per kick attempted in a penalty shootout.
# These kicks are separate from penalties taken during normal/extra time
# and are intentionally not included in the goals table.
penalty_kicks = (
    to_semantic_table(
        xo.deferred_read_parquet(f"{BASE_URL}/penalty_kicks.parquet"),
        name="penalty_kicks",
    )
    .with_dimensions(
        penalty_kick_id=entity_dimension(lambda t: t.penalty_kick_id),
        match_id=_.match_id,
        match_name=_.match_name,
        match_date=_.match_date,
        stage_name=_.stage_name,
        group_name=_.group_name,
        team_id=_.team_id,
        team_name=_.team_name,
        canonical_team_name=canonicalize_team_name(_.team_name),
        player_id=_.player_id,
        player_name=(_.given_name == "not applicable").ifelse(
            _.family_name, _.given_name + " " + _.family_name
        ),
        home_team=_.home_team,
        converted=_.converted,
    )
    .with_measures(
        attempt_count=_.count(),
        conversion_count=_.converted.sum(),
        miss_count=(_.converted == 0).sum(),
        shooter_count=_.player_id.nunique(),
        conversion_rate=lambda t: t.conversion_count.cast("float64") / t.attempt_count * 100,
    )
    .join_one(team_dimensions, on="team_id")
    .join_one(tournament_dimensions, on="tournament_id")
)


df1 = (
    matches.filter(lambda t: ~t.tournaments.womens)
    .group_by("tournaments.decade")
    .aggregate("matches.match_count", "matches.avg_goals_per_match")
    .order_by("tournaments.decade")
).to_tagged()

df2 = (
    team_appearances.group_by("team_appearances.canonical_team_name")
    .aggregate(
        "team_appearances.game_count",
        "team_appearances.win_count",
        "team_appearances.win_pct",
        "team_appearances.goals_for_total",
        "team_appearances.goal_difference",
    )
    .order_by(lambda t: t["team_appearances.win_count"].desc())
    .limit(10)
).to_tagged()

df3 = (
    team_appearances.group_by("teams.confederation_name")
    .aggregate("team_appearances.game_count", "team_appearances.win_pct")
    .order_by(lambda t: t["team_appearances.win_pct"].desc())
).to_tagged()

df4 = (
    goals.filter(lambda t: t.own_goal == 0)
    .group_by("goals.player_name")
    .aggregate("goals.goal_count", "goals.penalty_count")
    .order_by(lambda t: t["goals.goal_count"].desc())
    .limit(10)
).to_tagged()

df5 = (
    goals.group_by("goals.stage_name")
    .aggregate("goals.goal_count", "goals.pct_of_goals")
    .order_by(lambda t: t["goals.goal_count"].desc())
).to_tagged()

df6 = (
    matches.filter(lambda t: ~t.tournaments.womens)
    .group_by("tournaments.year")
    .aggregate(
        "matches.match_count",
        "matches.extra_time_count",
        "matches.shootout_count",
        "matches.avg_goals_per_match",
    )
    .order_by(lambda t: t["tournaments.year"].desc())
    .limit(8)
).to_tagged()

df7 = (
    penalty_kicks.group_by("penalty_kicks.canonical_team_name")
    .aggregate(
        "penalty_kicks.attempt_count",
        "penalty_kicks.conversion_count",
        "penalty_kicks.conversion_rate",
    )
    .filter(lambda t: t["penalty_kicks.attempt_count"] >= 10)
    .order_by(lambda t: t["penalty_kicks.conversion_rate"].desc())
    .limit(10)
).to_tagged()
