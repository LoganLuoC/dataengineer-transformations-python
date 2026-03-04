import math
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, radians, sin, cos, sqrt, atan2

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE


def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    """
    Computes the Haversine distance between start and end locations of each bike trip.
    
    The Haversine formula calculates the great-circle distance between two points
    on a sphere given their latitudes and longitudes.
    
    Args:
        _spark: SparkSession instance
        dataframe: Input DataFrame with start/end latitude and longitude columns
        
    Returns:
        DataFrame with added 'distance' column in miles
    """
    
    # Convert degrees to radians
    start_lat = radians(col("start_station_latitude"))
    start_lon = radians(col("start_station_longitude"))
    end_lat = radians(col("end_station_latitude"))
    end_lon = radians(col("end_station_longitude"))
    
    # Haversine formula implementation
    # Δσ = 2 × atan2(√a, √(1-a))
    # where a = sin²(Δφ/2) + cos(φ1) × cos(φ2) × sin²(Δλ/2)
    
    # Calculate differences
    dlat = end_lat - start_lat
    dlon = end_lon - start_lon
    
    # Haversine formula
    a = sin(dlat / 2) ** 2 + cos(start_lat) * cos(end_lat) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    # Distance in meters
    distance_meters = EARTH_RADIUS_IN_METERS * c
    
    # Convert meters to miles
    distance_miles = distance_meters / METERS_PER_MILE
    
    # Add distance column to dataframe
    return dataframe.withColumn("distance", distance_miles)


def run(
    spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str
) -> None:
    """
    Main execution function for the distance calculation job.
    
    Args:
        spark: SparkSession instance
        input_dataset_path: Path to input parquet files (citibike ingestion data)
        transformed_dataset_path: Path to output parquet files with distance column
    """
    # Read the ingested parquet data
    input_dataset = spark.read.parquet(input_dataset_path)
    
    print("Input dataset schema:")
    input_dataset.printSchema()
    print(f"\nInput dataset row count: {input_dataset.count()}")
    print("\nSample input data:")
    input_dataset.show(5)

    # Compute distances using Haversine formula
    dataset_with_distances = compute_distance(spark, input_dataset)
    
    print("\nDataset with computed distances:")
    dataset_with_distances.printSchema()
    print("\nSample output data:")
    dataset_with_distances.show(5)

    # Write the transformed dataset to parquet format
    dataset_with_distances.write.parquet(transformed_dataset_path, mode="append")
    
    print(f"\nTransformed dataset successfully written to {transformed_dataset_path}")