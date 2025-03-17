from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session

def create_events_aggregated_sink(t_env):
    table_name = 'rides_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            num_rides BIGINT
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



def create_events_rides_sink(t_env):
    table_name = 'taxi_rides_green_flink'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR ,
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance FLOAT,
            tip_amount FLOAT
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

def create_events_source_kafka(t_env):
    table_name = "taxi_rides_flink"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR ,
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance FLOAT,
            tip_amount FLOAT,
            event_watermark AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK for event_watermark as event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            # This lambda is your timestamp assigner:
            #   event -> The data record
            #   timestamp -> The previously assigned (or default) timestamp
            lambda event, timestamp: event[2]  # We treat the second tuple element as the event-time (ms).
        )
    )
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        source_sink = create_events_rides_sink(t_env)

        t_env.execute_sql(f"""INSERT INTO {source_sink} 
                          SELECT lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID, passenger_count, trip_distance, tip_amount 
                          from {source_table} """)



        aggregated_table = create_events_aggregated_sink(t_env)
        
        # use python API as SQL gives errors
        t_env.from_path(source_table).window(Session.with_gap(lit(5).minutes).on(col("event_watermark")).alias("w")
                                             ).group_by(col("w"), col("PULocationID"), col("DOLocationID")
                                                        ).select(col("w").start.alias("window_start"),
                                                                 col("w").end.alias("window_end"),
                                                                 col("PULocationID"),
                                                                 col("DOLocationID"),
                                                                 col("PULocationID").count.alias("num_rides")
                                                                 ).execute_insert(aggregated_table).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()