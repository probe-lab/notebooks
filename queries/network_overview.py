"""
Fetch functions for network overview analysis.

Each function executes SQL and writes directly to Parquet.
"""

from pathlib import Path


def _get_date_filter(target_date: str, column: str = "slot_start_date_time") -> str:
    """Generate SQL date filter for a specific date."""
    return f"{column} BETWEEN '{target_date}' AND '{target_date}'::date + INTERVAL 1 DAY"


def fetch_unique_network_participants_per_client(
    client,
    target_date: str,
    output_path: Path,
    network: str = "mainnet",
) -> int:
    """Fetch blobs per slot data and write to Parquet.

    Returns row count.
    """
    date_filter = _get_date_filter(target_date, column="event_date_time")

    query = f"""
SELECT
    toStartOfInterval(event_date_time, INTERVAL 1 hour) AS hour_bucket,
    remote_peer_id_unique_key as peer_id,
    remote_agent_implementation as client_name,
    meta_client_name as local_name
FROM libp2p_synthetic_heartbeat_local
WHERE
    meta_network_name LIKE 'mainnet'
  AND {date_filter}
ORDER BY hour_bucket ASC
"""

    df = client.query_df(query)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return len(df)

