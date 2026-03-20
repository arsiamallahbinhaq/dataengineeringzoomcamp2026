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


def create_tumbling_tips_sink(t_env):
    """Create PostgreSQL sink table untuk write hourly tip results."""
    table_name = "tumbling_tips"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            total_tip_amount DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
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


def tumbling_tips_aggregation():
    """
    Q6: Count trips per PULocationID dalam 1-hour tumbling window.
    
    Tumbling Window:
    - Fixed time windows (1 hour each)
    - Non-overlapping
    - Windows: [00:00, 01:00), [01:00, 02:00), etc.
    - Berguna untuk regular reporting pada interval tetap
    
    Process:
    1. Baca data dari Kafka topic green-trips
    2. Parse timestamp dari lpep_pickup_datetime
    3. Gunakan TUMBLE window function untuk 1-hour window
    4. GROUP BY window_start SAJA (total across all locations)
    5. SUM jumlah tip_amount per hour
    6. Write hasil ke PostgreSQL table
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Checkpoint setiap 10 detik
    env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Create source dan sink tables
    source_table = create_events_source_kafka(t_env)
    sink_table = create_tumbling_tips_sink(t_env)

    # Execute aggregation query dengan tumbling window
    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            SUM(tip_amount) AS total_tip_amount
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
        )
        GROUP BY window_start;
    """).wait()


if __name__ == '__main__':
    tumbling_tips_aggregation()
