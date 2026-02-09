"""
pipeline.consolidate — MERGE staging → master (idempotent upsert).
"""

from pipeline.db import STAGING_TABLE, MASTER_TABLE


def merge_staging_to_master(cursor):
    """MERGE staging → master (idempotent upsert)."""
    merge_sql = f"""
    MERGE INTO {MASTER_TABLE} AS tgt
    USING (
        SELECT
            LOC,
            MAX(LASTMOD) AS LASTMOD,
            LISTAGG(DISTINCT SOURCE_SITEMAP, ',') WITHIN GROUP (ORDER BY SOURCE_SITEMAP) AS SOURCES
        FROM {STAGING_TABLE}
        GROUP BY LOC
    ) AS src
    ON tgt.LOC = src.LOC

    WHEN MATCHED THEN UPDATE SET
        tgt.LAST_SEEN_AT = CURRENT_TIMESTAMP(),
        tgt.LASTMOD      = COALESCE(src.LASTMOD, tgt.LASTMOD),
        tgt.SOURCES      = (
            SELECT LISTAGG(DISTINCT val, ',') WITHIN GROUP (ORDER BY val)
            FROM (
                SELECT value AS val FROM TABLE(SPLIT_TO_TABLE(tgt.SOURCES, ','))
                UNION
                SELECT value AS val FROM TABLE(SPLIT_TO_TABLE(src.SOURCES, ','))
            )
        )

    WHEN NOT MATCHED THEN INSERT (
        LOC, LASTMOD, SOURCES, FIRST_SEEN_AT, LAST_SEEN_AT
    ) VALUES (
        src.LOC, src.LASTMOD, src.SOURCES, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );
    """
    cursor.execute(merge_sql)
    return cursor.fetchone()
