from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env):
    """Create Kafka source table untuk read green-trips data."""
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_session_pickup_sink(t_env):
    """Create PostgreSQL sink table untuk write session results."""
    table_name = "session_pickup"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (session_start, session_end, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def session_aggregation():
    """
    Main job: Count trips per PULocationID menggunakan session window.
    
    Session Window:
    - Groups events yang arrive dalam 5 menit satu sama lain
    - Ketika gap > 5 menit, window closes dan session baru dimulai
    - Berguna untuk detecting "user sessions" atau "activity streaks"
    
    Process:
    1. Baca data dari Kafka topic green-trips
    2. Parse timestamp dari lpep_pickup_datetime
    3. Gunakan SESSION window function dengan 5-minute gap
    4. GROUP BY session time boundary dan PULocationID
    5. COUNT jumlah trips per session
    6. Write hasil ke PostgreSQL table
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Use FileSystemCheckpointStorage for larger state handling
    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
    env.set_state_backend(EmbeddedRocksDBStateBackend(checkpoint_dir="/tmp/checkpoints"))
    
    env.enable_checkpointing(10 * 1000)  # Checkpoint setiap 10 detik
    env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Create source dan sink tables
    source_table = create_events_source_kafka(t_env)
    sink_table = create_session_pickup_sink(t_env)

    # Execute session aggregation query
    # SESSION window groups events within 5-minute inactivity gap
    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID;
    """).wait()


if __name__ == '__main__':
    session_aggregation()
