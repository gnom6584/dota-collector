from __future__ import annotations

MATCH_QUERY_FIELDS: str = """
id
didRadiantWin
durationSeconds
startDateTime
endDateTime
lobbyType
numHumanPlayers
gameMode
actualRank
averageRank
analysisOutcome
bottomLaneOutcome
midLaneOutcome
topLaneOutcome
gameVersionId
regionId
leagueId
rank
bracket
pickBans {
  isPick
  heroId
  bannedHeroId
  isRadiant
  wasBannedSuccessfully
  order
}
players {
  heroId
  isRadiant
  position
  roleBasic
  role
  lane
  kills
  deaths
  assists
  item0Id
  item1Id
  item2Id
  item3Id
  item4Id
  item5Id
  level
  goldPerMinute
  experiencePerMinute
  networth
  numLastHits
  numDenies
  heroDamage
  towerDamage
  intentionalFeeding
  award
  imp
}
""".strip()


def build_batch_matches_query(match_ids: list[int], fields: str = MATCH_QUERY_FIELDS) -> str:
    """Формирует пакетный GraphQL-запрос с алиасами для получения группы матчей.

    Args:
        match_ids: Список идентификаторов матчей Dota 2.
        fields: Набор полей GraphQL для каждого матча.

    Returns:
        Сформированная строка GraphQL-запроса вида `query { m<id>: match(id: <id>) { ... } }`.
    """
    fragments = [f"m{mid}: match(id: {mid}) {{\n{fields}\n}}" for mid in match_ids]
    return "query {\n" + "\n".join(fragments) + "\n}"
