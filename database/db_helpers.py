"""Small database helper functions extracted from `crud.py`.

These are thin wrappers that operate on a DB cursor and return ids
or None as appropriate. Kept minimal to stay easy to unit-test.
"""
from typing import Optional


def get_or_create_conference(cur, name: str, season_id: int) -> int:
    print(f"Getting or creating conference: {name} for season_id: {season_id}")
    cur.execute(
        """
        SELECT id FROM conferences WHERE name = %s AND season_id = %s
        """,
        (name, season_id),
    )
    result = cur.fetchone()
    if result:
        return result[0]
    cur.execute(
        """
        INSERT INTO conferences (name, season_id)
        VALUES (%s, %s)
        RETURNING id
        """,
        (name, season_id),
    )
    return cur.fetchone()[0]


def get_or_create_division(cur, name: str, conference_id: Optional[int], season_id: int) -> int:
    print(f"Getting or creating conference: {name} for season_id: {season_id}")
    if conference_id:
        cur.execute(
            """
            SELECT id FROM divisions WHERE name = %s AND conference_id = %s
            """,
            (name, conference_id),
        )
    else:
        cur.execute(
            """
            SELECT id FROM divisions WHERE name = %s AND conference_id IS NULL
            """,
            (name,),
        )

    result = cur.fetchone()
    if result:
        return result[0]

    if conference_id:
        cur.execute(
            """
            INSERT INTO divisions (name, conference_id, season_id)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (name, conference_id, season_id),
        )
    else:
        cur.execute(
            """
            INSERT INTO divisions (name, conference_id, season_id)
            VALUES (%s, NULL, %s)
            RETURNING id
            """,
            (name, season_id),
        )

    return cur.fetchone()[0]


def get_team_id(cur, abbreviation: str):
    cur.execute("SELECT id FROM teams WHERE abbreviation = %s", (abbreviation,))
    result = cur.fetchone()
    return result[0] if result else None

def get_team_season_id_from_team_name(cur, team_name, season_id):
    cur.execute("""
        SELECT ts.id, ts.team_id, ts.season_id, t.name
        FROM team_seasons ts
        JOIN teams t ON ts.team_id = t.id
        WHERE t.name = %s AND ts.season_id = %s
    """,(team_name, season_id))
    return cur.fetchall()
