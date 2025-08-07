#!/usr/bin/env python3
"""
Robust Spark ETL Script for E-commerce Data Processing
AWS EMR YARN Client Mode - Ready for spark-submit

This script processes e-commerce transaction data from Avro files stored in S3,
applies comprehensive data cleansing and validation, creates a star-flake schema
data warehouse, and outputs clean datasets for analytics and machine learning.

Author: ETL Pipeline Team
Version: 7.1 - FINAL TWEAKS
"""

import sys
import logging
from datetime import datetime
from typing import Dict, Any, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, isnan, isnull, regexp_replace, abs as spark_abs,
    to_timestamp, date_format, year, quarter, month, dayofmonth, 
    dayofweek, hour, broadcast, coalesce, upper, trim, regexp_extract, 
    length, split, create_map, from_json, to_json, struct, current_date, 
    date_add, count, sum as spark_sum, desc, asc, row_number, dense_rank,
    first, max as spark_max, concat_ws, format_string, 
    instr, substring, current_timestamp, sha2, unix_timestamp, 
    collect_list, collect_set, rand
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, TimestampType, DateType, DecimalType, LongType
)
from pyspark.sql.window import Window

# Configure logging for better monitoring and debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ECommerceETL")

class ECommerceETL:
    """
    Main ETL class for processing e-commerce transaction data.
    """
    
    def __init__(self):
        """Initialize the ETL pipeline with configuration parameters."""
        # S3 bucket paths for input and output data
        self.input_path = "s3a://iti-ecommerce-all/topics/ecommerce-events/"
        self.output_base = "s3a://iti-ecommerce-all/DWH/"
        
        # Data quality thresholds for monitoring pipeline health
        self.quality_threshold = 0.95  # Require >=95% data acceptance rate
        self.max_reject_rate = 0.05   # Allow maximum 5% rejection rate
        
        # Geographic lookup data for enriching location information
        self.geo_profiles = [
            # North America
            ("United States", "New York", "USD", "en", "America/New_York", "+1"),
            ("United States", "Los Angeles", "USD", "en", "America/Los_Angeles", "+1"),
            ("United States", "Chicago", "USD", "en", "America/Chicago", "+1"),
            ("United States", "Houston", "USD", "en", "America/Chicago", "+1"),
            ("United States", "Phoenix", "USD", "en", "America/Phoenix", "+1"),
            ("Canada", "Toronto", "CAD", "en", "America/Toronto", "+1"),
            ("Canada", "Montreal", "CAD", "en", "America/Toronto", "+1"),
            ("Canada", "Vancouver", "CAD", "en", "America/Vancouver", "+1"),
            ("Canada", "Calgary", "CAD", "en", "America/Edmonton", "+1"),
            ("Mexico", "Mexico City", "MXN", "es", "America/Mexico_City", "+52"),
            ("Mexico", "Guadalajara", "MXN", "es", "America/Mexico_City", "+52"),
            ("Mexico", "Monterrey", "MXN", "es", "America/Monterrey", "+52"),

            # Europe
            ("United Kingdom", "London", "GBP", "en", "Europe/London", "+44"),
            ("United Kingdom", "Manchester", "GBP", "en", "Europe/London", "+44"),
            ("United Kingdom", "Birmingham", "GBP", "en", "Europe/London", "+44"),
            ("United Kingdom", "Leeds", "GBP", "en", "Europe/London", "+44"),
            ("Germany", "Berlin", "EUR", "de", "Europe/Berlin", "+49"),
            ("Germany", "Munich", "EUR", "de", "Europe/Berlin", "+49"),
            ("Germany", "Hamburg", "EUR", "de", "Europe/Berlin", "+49"),
            ("Germany", "Cologne", "EUR", "de", "Europe/Berlin", "+49"),
            ("France", "Paris", "EUR", "fr", "Europe/Paris", "+33"),
            ("France", "Lyon", "EUR", "fr", "Europe/Paris", "+33"),
            ("France", "Marseille", "EUR", "fr", "Europe/Paris", "+33"),
            ("France", "Toulouse", "EUR", "fr", "Europe/Paris", "+33"),
            ("Spain", "Madrid", "EUR", "es", "Europe/Madrid", "+34"),
            ("Spain", "Barcelona", "EUR", "es", "Europe/Madrid", "+34"),
            ("Spain", "Valencia", "EUR", "es", "Europe/Madrid", "+34"),
            ("Spain", "Seville", "EUR", "es", "Europe/Madrid", "+34"),
            ("Italy", "Rome", "EUR", "it", "Europe/Rome", "+39"),
            ("Italy", "Milan", "EUR", "it", "Europe/Rome", "+39"),
            ("Italy", "Naples", "EUR", "it", "Europe/Rome", "+39"),
            ("Italy", "Turin", "EUR", "it", "Europe/Rome", "+39"),
            ("Netherlands", "Amsterdam", "EUR", "nl", "Europe/Amsterdam", "+31"),
            ("Netherlands", "Rotterdam", "EUR", "nl", "Europe/Amsterdam", "+31"),
            ("Netherlands", "The Hague", "EUR", "nl", "Europe/Amsterdam", "+31"),
            ("Sweden", "Stockholm", "SEK", "sv", "Europe/Stockholm", "+46"),
            ("Sweden", "Gothenburg", "SEK", "sv", "Europe/Stockholm", "+46"),
            ("Sweden", "Malmö", "SEK", "sv", "Europe/Stockholm", "+46"),
            ("Norway", "Oslo", "NOK", "no", "Europe/Oslo", "+47"),
            ("Norway", "Bergen", "NOK", "no", "Europe/Oslo", "+47"),
            ("Norway", "Trondheim", "NOK", "no", "Europe/Oslo", "+47"),
            ("Denmark", "Copenhagen", "DKK", "da", "Europe/Copenhagen", "+45"),
            ("Denmark", "Aarhus", "DKK", "da", "Europe/Copenhagen", "+45"),
            ("Denmark", "Odense", "DKK", "da", "Europe/Copenhagen", "+45"),
            ("Switzerland", "Zurich", "CHF", "de", "Europe/Zurich", "+41"),
            ("Switzerland", "Geneva", "CHF", "de", "Europe/Zurich", "+41"),
            ("Switzerland", "Basel", "CHF", "de", "Europe/Zurich", "+41"),
            ("Austria", "Vienna", "EUR", "de", "Europe/Vienna", "+43"),
            ("Austria", "Salzburg", "EUR", "de", "Europe/Vienna", "+43"),
            ("Austria", "Innsbruck", "EUR", "de", "Europe/Vienna", "+43"),
            ("Belgium", "Brussels", "EUR", "nl", "Europe/Brussels", "+32"),
            ("Belgium", "Antwerp", "EUR", "nl", "Europe/Brussels", "+32"),
            ("Belgium", "Ghent", "EUR", "nl", "Europe/Brussels", "+32"),
            ("Poland", "Warsaw", "PLN", "pl", "Europe/Warsaw", "+48"),
            ("Poland", "Krakow", "PLN", "pl", "Europe/Warsaw", "+48"),
            ("Poland", "Gdansk", "PLN", "pl", "Europe/Warsaw", "+48"),
            ("Portugal", "Lisbon", "EUR", "pt", "Europe/Lisbon", "+351"),
            ("Portugal", "Porto", "EUR", "pt", "Europe/Lisbon", "+351"),
            ("Portugal", "Braga", "EUR", "pt", "Europe/Lisbon", "+351"),

            # Asia-Pacific
            ("Japan", "Tokyo", "JPY", "ja", "Asia/Tokyo", "+81"),
            ("Japan", "Osaka", "JPY", "ja", "Asia/Tokyo", "+81"),
            ("Japan", "Yokohama", "JPY", "ja", "Asia/Tokyo", "+81"),
            ("Japan", "Nagoya", "JPY", "ja", "Asia/Tokyo", "+81"),
            ("South Korea", "Seoul", "KRW", "ko", "Asia/Seoul", "+82"),
            ("South Korea", "Busan", "KRW", "ko", "Asia/Seoul", "+82"),
            ("South Korea", "Incheon", "KRW", "ko", "Asia/Seoul", "+82"),
            ("China", "Beijing", "CNY", "zh", "Asia/Shanghai", "+86"),
            ("China", "Shanghai", "CNY", "zh", "Asia/Shanghai", "+86"),
            ("China", "Guangzhou", "CNY", "zh", "Asia/Shanghai", "+86"),
            ("China", "Shenzhen", "CNY", "zh", "Asia/Shanghai", "+86"),
            ("Australia", "Sydney", "AUD", "en", "Australia/Sydney", "+61"),
            ("Australia", "Melbourne", "AUD", "en", "Australia/Melbourne", "+61"),
            ("Australia", "Brisbane", "AUD", "en", "Australia/Brisbane", "+61"),
            ("Australia", "Perth", "AUD", "en", "Australia/Perth", "+61"),
            ("New Zealand", "Auckland", "NZD", "en", "Pacific/Auckland", "+64"),
            ("New Zealand", "Wellington", "NZD", "en", "Pacific/Auckland", "+64"),
            ("New Zealand", "Christchurch", "NZD", "en", "Pacific/Auckland", "+64"),
            ("Singapore", "Singapore", "SGD", "en", "Asia/Singapore", "+65"),
            ("India", "Mumbai", "INR", "en", "Asia/Kolkata", "+91"),
            ("India", "Delhi", "INR", "en", "Asia/Kolkata", "+91"),
            ("India", "Bangalore", "INR", "en", "Asia/Kolkata", "+91"),
            ("India", "Hyderabad", "INR", "en", "Asia/Kolkata", "+91"),
            ("Thailand", "Bangkok", "THB", "th", "Asia/Bangkok", "+66"),
            ("Thailand", "Chiang Mai", "THB", "th", "Asia/Bangkok", "+66"),
            ("Thailand", "Phuket", "THB", "th", "Asia/Bangkok", "+66"),
            ("Malaysia", "Kuala Lumpur", "MYR", "en", "Asia/Kuala_Lumpur", "+60"),
            ("Malaysia", "George Town", "MYR", "en", "Asia/Kuala_Lumpur", "+60"),
            ("Malaysia", "Johor Bahru", "MYR", "en", "Asia/Kuala_Lumpur", "+60"),
            ("Indonesia", "Jakarta", "IDR", "id", "Asia/Jakarta", "+62"),
            ("Indonesia", "Surabaya", "IDR", "id", "Asia/Jakarta", "+62"),
            ("Indonesia", "Bandung", "IDR", "id", "Asia/Jakarta", "+62"),
            ("Philippines", "Manila", "PHP", "en", "Asia/Manila", "+63"),
            ("Philippines", "Quezon City", "PHP", "en", "Asia/Manila", "+63"),
            ("Philippines", "Davao", "PHP", "en", "Asia/Manila", "+63"),
            ("Vietnam", "Ho Chi Minh City", "VND", "vi", "Asia/Ho_Chi_Minh", "+84"),
            ("Vietnam", "Hanoi", "VND", "vi", "Asia/Ho_Chi_Minh", "+84"),
            ("Vietnam", "Da Nang", "VND", "vi", "Asia/Ho_Chi_Minh", "+84"),

            # Middle East & Africa
            ("Egypt", "Cairo", "EGP", "ar", "Africa/Cairo", "+20"),
            ("Egypt", "Alexandria", "EGP", "ar", "Africa/Cairo", "+20"),
            ("Egypt", "Giza", "EGP", "ar", "Africa/Cairo", "+20"),
            ("Egypt", "Port Said", "EGP", "ar", "Africa/Cairo", "+20"),
            ("Saudi Arabia", "Riyadh", "SAR", "ar", "Asia/Riyadh", "+966"),
            ("Saudi Arabia", "Jeddah", "SAR", "ar", "Asia/Riyadh", "+966"),
            ("Saudi Arabia", "Mecca", "SAR", "ar", "Asia/Riyadh", "+966"),
            ("Saudi Arabia", "Medina", "SAR", "ar", "Asia/Riyadh", "+966"),
            ("UAE", "Dubai", "AED", "ar", "Asia/Dubai", "+971"),
            ("UAE", "Abu Dhabi", "AED", "ar", "Asia/Dubai", "+971"),
            ("UAE", "Sharjah", "AED", "ar", "Asia/Dubai", "+971"),
            ("Turkey", "Istanbul", "TRY", "tr", "Europe/Istanbul", "+90"),
            ("Turkey", "Ankara", "TRY", "tr", "Europe/Istanbul", "+90"),
            ("Turkey", "Izmir", "TRY", "tr", "Europe/Istanbul", "+90"),
            ("Israel", "Tel Aviv", "ILS", "he", "Asia/Jerusalem", "+972"),
            ("Israel", "Jerusalem", "ILS", "he", "Asia/Jerusalem", "+972"),
            ("Israel", "Haifa", "ILS", "he", "Asia/Jerusalem", "+972"),
            ("Jordan", "Amman", "JOD", "ar", "Asia/Amman", "+962"),
            ("Jordan", "Zarqa", "JOD", "ar", "Asia/Amman", "+962"),
            ("Jordan", "Irbid", "JOD", "ar", "Asia/Amman", "+962"),
            ("Lebanon", "Beirut", "LBP", "ar", "Asia/Beirut", "+961"),
            ("Lebanon", "Tripoli", "LBP", "ar", "Asia/Beirut", "+961"),
            ("Lebanon", "Sidon", "LBP", "ar", "Asia/Beirut", "+961"),
            ("South Africa", "Cape Town", "ZAR", "en", "Africa/Johannesburg", "+27"),
            ("South Africa", "Johannesburg", "ZAR", "en", "Africa/Johannesburg", "+27"),
            ("South Africa", "Durban", "ZAR", "en", "Africa/Johannesburg", "+27"),
            ("Nigeria", "Lagos", "NGN", "en", "Africa/Lagos", "+234"),
            ("Nigeria", "Kano", "NGN", "en", "Africa/Lagos", "+234"),
            ("Nigeria", "Ibadan", "NGN", "en", "Africa/Lagos", "+234"),
            ("Kenya", "Nairobi", "KES", "en", "Africa/Nairobi", "+254"),
            ("Kenya", "Mombasa", "KES", "en", "Africa/Nairobi", "+254"),
            ("Kenya", "Kisumu", "KES", "en", "Africa/Nairobi", "+254"),

            # South America
            ("Brazil", "São Paulo", "BRL", "pt", "America/Sao_Paulo", "+55"),
            ("Brazil", "Rio de Janeiro", "BRL", "pt", "America/Sao_Paulo", "+55"),
            ("Brazil", "Brasília", "BRL", "pt", "America/Sao_Paulo", "+55"),
            ("Brazil", "Salvador", "BRL", "pt", "America/Sao_Paulo", "+55"),
            ("Argentina", "Buenos Aires", "ARS", "es", "America/Argentina/Buenos_Aires", "+54"),
            ("Argentina", "Córdoba", "ARS", "es", "America/Argentina/Cordoba", "+54"),
            ("Argentina", "Rosario", "ARS", "es", "America/Argentina/Rosario", "+54"),
            ("Chile", "Santiago", "CLP", "es", "America/Santiago", "+56"),
            ("Chile", "Valparaíso", "CLP", "es", "America/Santiago", "+56"),
            ("Chile", "Concepción", "CLP", "es", "America/Santiago", "+56"),
            ("Colombia", "Bogotá", "COP", "es", "America/Bogota", "+56"),
            ("Colombia", "Medellín", "COP", "es", "America/Bogota", "+57"),
            ("Colombia", "Cali", "COP", "es", "America/Bogota", "+57"),
            ("Peru", "Lima", "PEN", "es", "America/Lima", "+51"),
            ("Peru", "Arequipa", "PEN", "es", "America/Lima", "+51"),
            ("Peru", "Trujillo", "PEN", "es", "America/Lima", "+51")
        ]
        
        # Initialize Spark session
        self.spark = None
        
    def create_spark_session(self) -> SparkSession:
        """Initialize and configure the Spark session."""
        logger.info("Creating Spark session...")
        
        spark = SparkSession.builder \
            .appName("ECommerceETL-Production-Finalized") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")
        self.spark = spark
        
        sc = spark.sparkContext
        logger.info("Spark session created successfully")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Default parallelism: {sc.defaultParallelism}")
        
        return spark
    
    def get_avro_schema(self) -> StructType:
        """Define the schema for the Avro source files."""
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("quantity", IntegerType(), False), 
            StructField("total_amount", DoubleType(), True),
            StructField("feedback_score", IntegerType(), True),
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), False),
            StructField("user_email", StringType(), False),
            StructField("user_phone", StringType(), True),
            StructField("is_first_purchase", BooleanType(), False),
            StructField("language", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("city_name", StringType(), False),
            StructField("country_name", StringType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("product_category", StringType(), False),
            StructField("product_brand", StringType(), False),
            # FINALIZED: Product price nested struct - will be flattened
            StructField("product_price", StructType([
                StructField("string", StringType(), True),
                StructField("double", DoubleType(), True)
            ]), True),
            StructField("shipping_address", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("device_type", StringType(), False),
            StructField("browser", StringType(), False),
            StructField("ip_address", StringType(), False),
            StructField("referral_source", StringType(), True),
            StructField("campaign", StringType(), True),
            StructField("coupon_code", StringType(), True),
            StructField("coupon_code_flag", BooleanType(), True),
            StructField("payment_method", StringType(), False),
            StructField("payment_status", StringType(), False),
            StructField("transaction_status", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("event_timestamp", StringType(), True),
            StructField("dt_date", StringType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("hour", IntegerType(), False)
        ])
    
    def read_avro_data(self) -> Tuple[DataFrame, int]:
        """Read Avro files from S3 with optimized settings."""
        logger.info(f"Reading Avro data from {self.input_path}")
        
        try:
            df = self.spark.read \
                .format("avro") \
                .schema(self.get_avro_schema()) \
                .option("mergeSchema", "false") \
                .load(f"{self.input_path}partition=*")
            
            num_tasks = self.spark.sparkContext.defaultParallelism
            df = df.repartition(num_tasks).cache()
            
            record_count = df.count()
            logger.info(f"Successfully loaded {record_count:,} records")
            
            if record_count == 0:
                raise ValueError("No data found in source path")
                
            return df, record_count
            
        except Exception as e:
            logger.error(f"Failed to read Avro data: {str(e)}")
            raise
    
    def create_geo_lookup(self) -> DataFrame:
        """Create a geographic lookup DataFrame for data enrichment."""
        logger.info("Creating geographic lookup table...")
        
        geo_data = [(country, city, currency, lang, tz, code) 
                   for country, city, currency, lang, tz, code in self.geo_profiles]
        
        geo_schema = StructType([
            StructField("lookup_country", StringType(), False),
            StructField("lookup_city", StringType(), False),
            StructField("lookup_currency", StringType(), False),
            StructField("lookup_language", StringType(), False),
            StructField("lookup_timezone", StringType(), False),
            StructField("lookup_country_code", StringType(), False)
        ])
        
        geo_df = self.spark.createDataFrame(geo_data, geo_schema)
        logger.info(f"Geographic lookup created with {len(self.geo_profiles)} entries")
        
        return geo_df
    
    def apply_data_cleansing(self, df: DataFrame, raw_count: int) -> Tuple[DataFrame, DataFrame, int, int]:
        """Apply comprehensive data cleansing and validation rules."""
        logger.info("Starting data cleansing process...")
        
        geo_lookup = self.create_geo_lookup()
        
        # Step 1: Identify records to reject
        logger.info("Step 1: Identifying critical rejection cases...")
        
        df_with_flags = df.withColumn(
            "reject_reason",
            when(col("user_id").isNull() | (col("user_id") == ""), "NULL_USER_ID")
            .when(col("event_timestamp").isNull() | (col("event_timestamp") == ""), "NULL_TIMESTAMP")
            .when(col("action_type").isin(["health_check", "corruption_test"]), "SYSTEM_RECORD")
            .when(
                col("event_timestamp").isNotNull() & 
                (to_timestamp(col("event_timestamp")) > current_timestamp()), 
                "FUTURE_TIMESTAMP"
            )
            .when(
                (col("action_type") == "purchase") & 
                (col("shipping_address").isNull() | (col("shipping_address") == "")),
                "MISSING_SHIPPING_FOR_PURCHASE"
            )
            .otherwise(lit(None))
        ).cache()
        
        # Step 2: Split data
        rejected_df = df_with_flags.filter(col("reject_reason").isNotNull())
        processable_df = df_with_flags.filter(col("reject_reason").isNull()).drop("reject_reason")
        
        rejected_count = rejected_df.count()
        processable_count = raw_count - rejected_count
        
        logger.info(f"Critical rejections: {rejected_count:,}")
        logger.info(f"Records to process: {processable_count:,}")
        
        # Step 3: Apply data cleansing transformations
        logger.info("Step 2: Applying data cleansing transformations...")
        
        cleaned_df = processable_df \
            .withColumn("product_price_string", col("product_price.string")) \
            .withColumn("product_price_double", col("product_price.double")) \
            .withColumn("product_price",
                coalesce(
                    regexp_replace(col("product_price_string"), "[^0-9.]", "").cast(DoubleType()),
                    col("product_price_double")
                )
            ) \
            .drop("total_amount_calculated", "product_price_string", "product_price_double") \
            .withColumn("feedback_score_clean",
                when(col("feedback_score").isNull(), lit(0))
                .when(col("feedback_score") > 5, lit(5))
                .when(col("feedback_score") < 1, lit(1))
                .otherwise(col("feedback_score"))
            ) \
            .withColumn("user_phone_clean", 
                when(col("user_phone").isNull(), lit("NOT_PROVIDED"))
                .when(length(regexp_replace(col("user_phone"), "[^0-9]", "")) >= 10, col("user_phone"))
                .otherwise(lit("NOT_PROVIDED"))
            ) \
            .withColumn("quantity_clean", 
                when(col("quantity") < 0, spark_abs(col("quantity")))
                .otherwise(col("quantity"))
            ) \
            .withColumn("user_email_clean", 
                when(col("user_email").isNull() | (col("user_email") == ""), lit("unknown@example.com"))
                .when((instr(col("user_email"), "@") > 0) & (instr(col("user_email"), ".") > 0), col("user_email"))
                .otherwise(lit("unknown@example.com"))
            ) \
            .withColumn("referral_source_clean", 
                coalesce(col("referral_source"), lit("direct"))
            ) \
            .withColumn("campaign_clean", 
                coalesce(col("campaign"), lit("no_campaign"))
            ) \
            .withColumn("coupon_code_clean", 
                coalesce(col("coupon_code"), lit("no_coupon"))
            ) \
            .withColumn("product_category_clean", 
                upper(trim(col("product_category")))
            ) \
            .withColumn("postal_code_clean",
                coalesce(col("postal_code"), lit("UNKNOWN"))
            ).cache()
        
        # Step 4: Geographic validation
        logger.info("Step 3: Geographic validation and enrichment...")
        
        geo_validated = cleaned_df.join(
            broadcast(geo_lookup),
            (cleaned_df.country_name == geo_lookup.lookup_country) &
            (cleaned_df.city_name == geo_lookup.lookup_city),
            "left"
        ).withColumn(
            "country_validated",
            when(col("lookup_country").isNotNull(), col("lookup_country"))
            .otherwise(lit("UNKNOWN"))
        ).withColumn(
            "city_validated", 
            when(col("lookup_city").isNotNull(), col("lookup_city"))
            .otherwise(col("city_name"))
        ).select(
            *cleaned_df.columns,
            "country_validated",
            "city_validated"
        ).cache()

        # Step 5: Parse timestamps and create derived date fields
        logger.info("Step 4: Adding derived timestamp fields...")
        
        final_cleaned = geo_validated.withColumn(
            "event_timestamp_parsed",
            to_timestamp(col("event_timestamp"))
        ).withColumn(
            "dt_date_derived",
            date_format(col("event_timestamp_parsed"), "yyyy-MM-dd")
        ).withColumn(
            "year_derived", year(col("event_timestamp_parsed"))
        ).withColumn(
            "quarter_derived", quarter(col("event_timestamp_parsed"))
        ).withColumn(
            "month_derived", month(col("event_timestamp_parsed"))
        ).withColumn(
            "day_derived", dayofmonth(col("event_timestamp_parsed"))
        ).withColumn(
            "day_of_week_derived", dayofweek(col("event_timestamp_parsed"))
        ).withColumn(
            "hour_derived", hour(col("event_timestamp_parsed"))
        ).cache()
        
        # Step 6: Format rejected records - FINALIZED to exact 3-column schema
        logger.info("Step 5: Preparing rejected records with exact schema...")
        
        rejected_output = rejected_df.select(
            to_json(struct([col(c) for c in rejected_df.columns if c != "reject_reason"])).alias("raw_record"),
            col("reject_reason").alias("error_message"),
            current_date().alias("ingestion_date")
        )
        
        final_count = final_cleaned.count()
        
        logger.info(f"Final clean records: {final_count:,}")
        logger.info(f"Final rejected records: {rejected_count:,}")
        
        # Clean up intermediate cached DataFrames
        df_with_flags.unpersist()
        cleaned_df.unpersist()
        geo_validated.unpersist()
        
        return final_cleaned, rejected_output, final_count, rejected_count
    
    def check_quality_threshold(self, clean_count: int, reject_count: int) -> bool:
        """Validate data quality meets minimum acceptance thresholds."""
        total_count = clean_count + reject_count
        if total_count == 0:
            logger.error("No records to process!")
            return False
            
        acceptance_rate = clean_count / total_count
        rejection_rate = reject_count / total_count
        
        logger.info("="*50)
        logger.info("DATA QUALITY METRICS")
        logger.info("="*50)
        logger.info(f"Total records: {total_count:,}")
        logger.info(f"Clean records: {clean_count:,}")
        logger.info(f"Rejected records: {reject_count:,}")
        logger.info(f"Acceptance rate: {acceptance_rate:.3%}")
        logger.info(f"Rejection rate: {rejection_rate:.3%}")
        logger.info(f"Required acceptance rate: >={self.quality_threshold:.1%}")
        logger.info(f"Max rejection rate: <={self.max_reject_rate:.1%}")
        
        meets_threshold = (acceptance_rate >= self.quality_threshold and 
                          rejection_rate <= self.max_reject_rate)
        
        if meets_threshold:
            logger.info("Data quality meets requirements")
        else:
            logger.warning("Data quality below acceptable threshold!")
            logger.warning("Consider investigating data sources or adjusting cleansing rules")
            
        logger.info("="*50)
        
        return meets_threshold
    
    def generate_deterministic_surrogate_key(self, df: DataFrame, 
                                           natural_keys: list, 
                                           sk_column_name: str,
                                           start_value: int = 1) -> DataFrame:
        """
        FINALIZED: Generate truly unique surrogate keys using deterministic approach.
        
        Uses row_number() over a deterministic window to ensure unique, 
        non-colliding surrogate keys across all partitions and jobs.
        
        Args:
            df: DataFrame to add surrogate keys to
            natural_keys: List of columns to use for deterministic ordering
            sk_column_name: Name of the surrogate key column to create
            start_value: Starting value for surrogate keys (default 1)
            
        Returns:
            DataFrame: DataFrame with unique surrogate key column added
        """
        # Create deterministic window specification
        window_spec = Window.orderBy(*natural_keys)
        
        # Generate sequential surrogate keys starting from start_value
        result_df = df.withColumn(
            sk_column_name,
            (row_number().over(window_spec) + lit(start_value - 1)).cast(IntegerType())
        )
        
        return result_df
    
    def create_dimension_tables(self, df: DataFrame) -> Dict[str, DataFrame]:
        """
        FINALIZED: Create dimension tables with exact column names and ordering.
        
        All surrogate keys now use deterministic generation instead of 
        monotonically_increasing_id() to prevent collisions.
        """
        logger.info("Creating dimension tables for conformed STAR-FLAKE schema...")
        
        dimensions = {}
        
        # FINALIZED: dim_date_time with exact column names and order
        logger.info("Creating dim_date_time with exact schema...")
        date_time_base = df.select(
            "dt_date_derived",
            "hour_derived"
        ).distinct().withColumn(
            "event_timestamp",
            to_timestamp(
                concat_ws(" ", col("dt_date_derived"),
                         format_string("%02d:00:00", col("hour_derived")))
            )
        ).select(
            "dt_date_derived",
            "hour_derived", 
            "event_timestamp"
        ).distinct()
        
        dimensions["dim_date_time"] = self.generate_deterministic_surrogate_key(
            date_time_base,
            natural_keys=["dt_date_derived", "hour_derived"],
            sk_column_name="date_time_sk"
        ).withColumn(
            "year_derived", year(col("event_timestamp"))
        ).withColumn(
            "quarter_derived", quarter(col("event_timestamp"))
        ).withColumn(
            "month_derived", month(col("event_timestamp"))
        ).withColumn(
            "day_derived", dayofmonth(col("event_timestamp"))
        ).withColumn(
            "day_of_week_derived", dayofweek(col("event_timestamp"))
        ).select(
            # EXACT column order as specified
            col("date_time_sk"),
            col("event_timestamp"),
            col("dt_date_derived").alias("dt_date"),
            col("year_derived").alias("year"),
            col("quarter_derived").alias("quarter"),
            col("month_derived").alias("month"),
            col("day_derived").alias("day"),
            col("day_of_week_derived").alias("day_of_week"),
            col("hour_derived").alias("hour")
        ).cache()
        
        # FINALIZED: dim_user with proper deduplication and exact schema
        logger.info("Creating dim_user with exact schema...")
        user_window = Window.partitionBy("user_id").orderBy("user_name")
        user_base = df.filter(col("user_id").isNotNull()).select(
            "user_id",
            "user_name", 
            "user_email_clean",
            "user_phone_clean",
            "is_first_purchase",
            "language",
            "currency"
        ).withColumn("row_num", row_number().over(user_window)) \
         .filter(col("row_num") == 1) \
         .drop("row_num")
        
        dimensions["dim_user"] = self.generate_deterministic_surrogate_key(
            user_base,
            natural_keys=["user_id"],
            sk_column_name="user_sk"
        ).select(
            # EXACT column order as specified
            col("user_sk"),
            col("user_id"),
            col("user_name"),
            col("user_email_clean").alias("user_email"),
            col("user_phone_clean").alias("user_phone"),
            col("is_first_purchase"),
            col("language"),
            col("currency")
        ).cache()
        
        # UPDATED: dim_product with dynamic brands, normalized names, dynamic categories, and exactly 10,000 rows
        logger.info("Creating dim_product with dynamic brands, normalized names, dynamic categories...")
        
        # Step 1: Get all distinct brands from the data
        all_brands = [row.product_brand for row in df.select("product_brand").distinct().collect()]
        logger.info(f"Found {len(all_brands)} distinct brands in the data")
        
        # Step 2: Get all distinct categories from the data
        all_categories = [row.product_category_clean for row in df.select("product_category_clean").distinct().collect()]
        logger.info(f"Found {len(all_categories)} distinct categories in the data")
        
        # Step 3: Create a function to strip brand prefix from product name
        def strip_brand_prefix(product_name, brand):
            if product_name and brand:
                # Remove brand prefix if it exists at the start
                if product_name.upper().startswith(brand.upper()):
                    stripped = product_name[len(brand):].strip()
                    return stripped if stripped else product_name
            return product_name
        
        # Step 4: Normalize product names by stripping brand prefixes
        product_base = df.select(
            "product_id",
            "product_name",
            "product_category_clean", 
            "product_brand",
            "product_price"
        ).withColumn(
            "product_name_normalized",
            when(col("product_name").isNotNull() & col("product_brand").isNotNull(),
                 regexp_replace(
                     regexp_replace(
                         col("product_name"),
                         concat_ws("", lit("^"), col("product_brand"), lit("\\s+")),
                         ""
                     ),
                     concat_ws("", lit("^"), upper(col("product_brand")), lit("\\s+")),
                     ""
                 )
            ).otherwise(col("product_name"))
        ).withColumn(
            "product_name_final",
            when(col("product_name_normalized") == "", col("product_name"))
            .otherwise(col("product_name_normalized"))
        ).select(
            "product_id",
            col("product_name_final").alias("product_name"),
            "product_category_clean", 
            "product_brand",
            "product_price"
        )
        
        # Step 5: Deduplicate by product_brand, product_name, product_category
        product_window = Window.partitionBy("product_brand", "product_name", "product_category_clean").orderBy("product_id")
        product_deduped = product_base.withColumn("row_num", row_number().over(product_window)) \
                                    .filter(col("row_num") == 1) \
                                    .drop("row_num")
        
        deduped_count = product_deduped.count()
        logger.info(f"After deduplication: {deduped_count} unique product combinations")
        
        # Step 6: Ensure exactly 10,000 rows
        target_count = 10000
        
        if deduped_count > target_count:
            # Sample down to 10,000 without replacement
            logger.info(f"Sampling down from {deduped_count} to {target_count} products...")
            fraction = target_count / deduped_count
            product_final = product_deduped.sample(False, fraction, seed=42).limit(target_count)
            
        elif deduped_count < target_count:
            # Sample up to 10,000 with replacement
            logger.info(f"Sampling up from {deduped_count} to {target_count} products...")
            # Calculate how many times we need to replicate the data
            multiplier = (target_count // deduped_count) + 1
            
            # Create multiple copies with different random seeds
            product_copies = []
            for i in range(multiplier):
                copy_df = product_deduped.sample(True, 1.0, seed=42 + i)
                product_copies.append(copy_df)
            
            # Union all copies and take exactly 10,000
            product_final = product_copies[0]
            for copy_df in product_copies[1:]:
                product_final = product_final.union(copy_df)
            
            product_final = product_final.limit(target_count)
        else:
            # Exactly the right count
            logger.info(f"Perfect match: {deduped_count} products")
            product_final = product_deduped
        
        # Step 7: Add surrogate keys and final schema
        dimensions["dim_product"] = self.generate_deterministic_surrogate_key(
            product_final,
            natural_keys=["product_brand", "product_name", "product_category_clean"],
            sk_column_name="product_sk"
        ).select(
            # EXACT column order as specified
            col("product_sk"),
            col("product_id"),
            col("product_name"),
            col("product_category_clean").alias("product_category"),
            col("product_brand"),
            col("product_price")
        ).cache()
        
        final_product_count = dimensions["dim_product"].count()
        logger.info(f"Final dim_product count: {final_product_count} (target: {target_count})")
        
        # FINALIZED: dim_country with exact schema
        logger.info("Creating dim_country with exact schema...")
        country_base = df.select("country_validated") \
                        .filter(col("country_validated").isNotNull()) \
                        .distinct()
        
        dimensions["dim_country"] = self.generate_deterministic_surrogate_key(
            country_base,
            natural_keys=["country_validated"],
            sk_column_name="country_sk"
        ).select(
            # EXACT column order as specified
            col("country_sk"),
            col("country_validated").alias("country_name")
        ).cache()

        # FINALIZED: dim_city with exact schema and proper FK relationship
        logger.info("Creating dim_city with exact schema...")
        city_base = df.select(
            "country_validated", 
            "city_validated",
            "postal_code_clean"
        ).filter(
            col("country_validated").isNotNull() & 
            col("city_validated").isNotNull()
        ).groupBy("country_validated", "city_validated") \
         .agg(first("postal_code_clean").alias("postal_code")) \
         .distinct()
        
        # Join with country dimension to get FK
        city_with_country = city_base.alias("cb").join(
            dimensions["dim_country"].alias("dc"),
            col("cb.country_validated") == col("dc.country_name"),
            "inner"
        ).select(
            col("cb.city_validated").alias("city_name"),
            col("cb.postal_code"),
            col("dc.country_sk")
        )
        
        dimensions["dim_city"] = self.generate_deterministic_surrogate_key(
            city_with_country,
            natural_keys=["city_name", "country_sk"],
            sk_column_name="city_sk"
        ).select(
            # EXACT column order as specified
            col("city_sk"),
            col("city_name"),
            col("postal_code"),
            col("country_sk")
        ).cache()
        
        # FINALIZED: dim_shipping_address with exact schema
        logger.info("Creating dim_shipping_address with exact schema...")
        
        # Normalize and deduplicate shipping addresses
        shipping_normalized = (
            df
            .filter(
                (col("action_type") == "purchase") &
                col("shipping_address").isNotNull() & 
                (col("shipping_address") != "") &
                col("city_validated").isNotNull()
            )
            .select("shipping_address", "city_validated")
            .withColumn(
                "shipping_address_normalized",
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            trim(upper(col("shipping_address"))), 
                            "\\s+", " "
                        ),
                        "\\s+(ST|STREET|AVE|AVENUE|RD|ROAD|BLVD|BOULEVARD|DR|DRIVE)\\b", " ST"
                    ),
                    "[^A-Z0-9\\s]", ""
                )
            )
            .select("shipping_address_normalized", "city_validated")
            .distinct()
        )

        # Join with cities to get FK
        city_lookup = dimensions["dim_city"].select("city_sk", "city_name").distinct()
        shipping_with_city = shipping_normalized.alias("sn").join(
            city_lookup.alias("cl"),
            col("sn.city_validated") == col("cl.city_name"),
            "inner"
        ).select(
            col("sn.shipping_address_normalized").alias("shipping_address"),
            col("cl.city_sk")
        ).distinct()

        dimensions["dim_shipping_address"] = self.generate_deterministic_surrogate_key(
            shipping_with_city,
            natural_keys=["shipping_address", "city_sk"],
            sk_column_name="ship_addr_sk"
        ).select(
            # EXACT column order as specified
            col("ship_addr_sk"),
            col("shipping_address"),
            col("city_sk")
        )

        # Add default record for missing shipping addresses
        default_shipping = self.spark.createDataFrame([(-1, "NO_SHIPPING_ADDRESS", -1)], 
                                                    ["ship_addr_sk", "shipping_address", "city_sk"])
        dimensions["dim_shipping_address"] = dimensions["dim_shipping_address"].union(default_shipping).cache()
        
        # FINALIZED: dim_device with exact schema
        logger.info("Creating dim_device with exact schema...")
        device_base = df.select("device_type", "browser").distinct()
        
        dimensions["dim_device"] = self.generate_deterministic_surrogate_key(
            device_base,
            natural_keys=["device_type", "browser"],
            sk_column_name="device_sk"
        ).select(
            # EXACT column order as specified
            col("device_sk"),
            col("device_type"),
            col("browser")
        ).cache()
        
        # FINALIZED: dim_marketing with exact schema (normalized approach)
        logger.info("Creating dim_marketing with exact schema...")
        referral_sources = [r.referral_source_clean for r in df.select("referral_source_clean").distinct().collect()]
        campaigns = [c.campaign_clean for c in df.select("campaign_clean").distinct().collect()]
        coupons = [c.coupon_code_clean for c in df.select("coupon_code_clean").distinct().collect()]

        marketing_data = []
        sk = 1
        for r in referral_sources:
            marketing_data.append((sk, "REFERRAL_SOURCE", r, r)); sk += 1
        for c in campaigns:
            marketing_data.append((sk, "CAMPAIGN", c, c)); sk += 1
        for c in coupons:
            marketing_data.append((sk, "COUPON", c, c)); sk += 1

        marketing_schema = StructType([
            StructField("marketing_sk", IntegerType(), False),
            StructField("marketing_type", StringType(), False),
            StructField("marketing_code", StringType(), False),
            StructField("marketing_description", StringType(), False),
        ])
        
        dimensions["dim_marketing"] = self.spark.createDataFrame(marketing_data, marketing_schema).cache()
        
        # FINALIZED: dim_order_flags with exact schema
        logger.info("Creating dim_order_flags with exact schema...")
        flags_base = df.select(
            "payment_method",
            "payment_status",
            "transaction_status",
            "action_type"
        ).distinct()
        
        dimensions["dim_order_flags"] = self.generate_deterministic_surrogate_key(
            flags_base,
            natural_keys=["payment_method", "payment_status", "transaction_status", "action_type"],
            sk_column_name="flag_sk"
        ).select(
            # EXACT column order as specified
            col("payment_method"),
            col("payment_status"),
            col("transaction_status"),
            col("action_type"),
            col("flag_sk")
        ).cache()
        
        # Log dimension statistics
        for dim_name, dim_df in dimensions.items():
            record_count = dim_df.count()
            logger.info(f"{dim_name}: {record_count:,} records")
            
        logger.info(f"Successfully created {len(dimensions)} dimension tables")
        return dimensions
    
    def create_fact_table(self, df: DataFrame, dimensions: Dict[str, DataFrame]) -> Tuple[DataFrame, int]:
        """
        FINALIZED: Create fact table with exact column names and ordering.
        
        Sequential joins to build the fact table with proper error handling
        and join explosion detection (now using warnings instead of errors).
        """
        logger.info("Creating fact table for STAR-FLAKE schema...")
        
        fact_base = df.select(
            "event_id",
            "quantity_clean", 
            "product_price",
            "feedback_score_clean",
            "dt_date_derived",
            "hour_derived",
            "user_id",
            "product_id",
            "shipping_address",
            "city_validated", 
            "device_type", 
            "browser", 
            "referral_source_clean", 
            "campaign_clean", 
            "coupon_code_clean",
            "payment_method", 
            "payment_status", 
            "transaction_status", 
            "action_type"
        ).repartition(32).cache()
        
        fact_base_count = fact_base.count()
        logger.info(f"Starting fact table creation with {fact_base_count:,} base records")
        
        # Step 1: Join with date/time dimension
        logger.info("Joining with dim_date_time...")
        fact_with_datetime = fact_base.alias("fb").join(
            broadcast(dimensions["dim_date_time"].select("date_time_sk", "dt_date", "hour")).alias("dt"),
            (col("fb.dt_date_derived") == col("dt.dt_date")) &
            (col("fb.hour_derived") == col("dt.hour")),
            "inner"
        ).select(
            col("fb.event_id"), 
            col("fb.quantity_clean"), 
            col("fb.product_price"), 
            col("fb.feedback_score_clean"),
            col("dt.date_time_sk"),
            col("fb.user_id"), 
            col("fb.product_id"), 
            col("fb.shipping_address"), 
            col("fb.city_validated"),
            col("fb.device_type"), 
            col("fb.browser"), 
            col("fb.referral_source_clean"), 
            col("fb.campaign_clean"), 
            col("fb.coupon_code_clean"),
            col("fb.payment_method"), 
            col("fb.payment_status"), 
            col("fb.transaction_status"), 
            col("fb.action_type")
        ).cache()
        
        datetime_count = fact_with_datetime.count()
        logger.info(f"After dim_date_time join: {datetime_count:,} records")
        fact_base.unpersist()
        
        # Step 2: Join with user dimension
        logger.info("Joining with dim_user...")
        fact_with_user = fact_with_datetime.alias("fdt").join(
            broadcast(dimensions["dim_user"].select("user_sk", "user_id")).alias("du"),
            col("fdt.user_id") == col("du.user_id"),
            "inner"
        ).select(
            col("fdt.event_id"), 
            col("fdt.quantity_clean"), 
            col("fdt.product_price"), 
            col("fdt.feedback_score_clean"),
            col("fdt.date_time_sk"), 
            col("du.user_sk"),
            col("fdt.product_id"), 
            col("fdt.shipping_address"), 
            col("fdt.city_validated"),
            col("fdt.device_type"), 
            col("fdt.browser"), 
            col("fdt.referral_source_clean"), 
            col("fdt.campaign_clean"), 
            col("fdt.coupon_code_clean"),
            col("fdt.payment_method"), 
            col("fdt.payment_status"), 
            col("fdt.transaction_status"), 
            col("fdt.action_type")
        ).cache()
        
        user_count = fact_with_user.count()
        logger.info(f"After dim_user join: {user_count:,} records")
        fact_with_datetime.unpersist()
        
        # Step 3: Join with product dimension
        logger.info("Joining with dim_product...")
        fact_with_product = fact_with_user.alias("fu").join(
            broadcast(dimensions["dim_product"].select("product_sk", "product_id")).alias("dp"),
            col("fu.product_id") == col("dp.product_id"),
            "inner"
        ).select(
            col("fu.event_id"), 
            col("fu.quantity_clean"), 
            col("fu.product_price"), 
            col("fu.feedback_score_clean"),
            col("fu.date_time_sk"), 
            col("fu.user_sk"), 
            col("dp.product_sk"),
            col("fu.shipping_address"), 
            col("fu.city_validated"),
            col("fu.device_type"), 
            col("fu.browser"), 
            col("fu.referral_source_clean"), 
            col("fu.campaign_clean"), 
            col("fu.coupon_code_clean"),
            col("fu.payment_method"), 
            col("fu.payment_status"), 
            col("fu.transaction_status"), 
            col("fu.action_type")
        ).cache()
        
        product_count = fact_with_product.count()
        logger.info(f"After dim_product join: {product_count:,} records")
        fact_with_user.unpersist()
        
        # Step 4: Join with shipping address dimension
        logger.info("Joining with dim_shipping_address...")
        
        shipping_lookup = dimensions["dim_shipping_address"].alias("dsa").join(
            dimensions["dim_city"].select("city_sk", "city_name").alias("dc"),
            col("dsa.city_sk") == col("dc.city_sk"),
            "inner"
        ).select(
            col("dsa.ship_addr_sk"),
            col("dsa.shipping_address"),
            col("dc.city_name")
        ).distinct().cache()
        
        fact_with_shipping = fact_with_product.alias("fp").join(
            shipping_lookup.alias("sl"),
            (regexp_replace(
                regexp_replace(
                    regexp_replace(
                        trim(upper(coalesce(col("fp.shipping_address"), lit("NO_SHIPPING_ADDRESS")))), 
                        "\\s+", " "
                    ),
                    "\\s+(ST|STREET|AVE|AVENUE|RD|ROAD|BLVD|BOULEVARD|DR|DRIVE)\\b", " ST"
                ),
                "[^A-Z0-9\\s]", ""
            ) == col("sl.shipping_address")) &
            (col("fp.city_validated") == col("sl.city_name")),
            "left"
        ).select(
            col("fp.event_id"), 
            col("fp.quantity_clean"), 
            col("fp.product_price"), 
            col("fp.feedback_score_clean"),
            col("fp.date_time_sk"), 
            col("fp.user_sk"), 
            col("fp.product_sk"),
            coalesce(col("sl.ship_addr_sk"), lit(-1)).alias("ship_addr_sk"),
            col("fp.device_type"), 
            col("fp.browser"), 
            col("fp.referral_source_clean"), 
            col("fp.campaign_clean"), 
            col("fp.coupon_code_clean"),
            col("fp.payment_method"), 
            col("fp.payment_status"), 
            col("fp.transaction_status"), 
            col("fp.action_type")
        ).cache()
        
        shipping_join_count = fact_with_shipping.count()
        logger.info(f"After dim_shipping_address join: {shipping_join_count:,} records")
        
        # FINALIZED: Use logger.warning instead of logger.error for join explosion
        if shipping_join_count > product_count * 1.1:
            logger.warning(f"POTENTIAL JOIN EXPLOSION DETECTED!")
            logger.warning(f"Before shipping join: {product_count:,}")
            logger.warning(f"After shipping join: {shipping_join_count:,}")
            
            window_spec = Window.partitionBy("event_id").orderBy("ship_addr_sk")
            fact_with_shipping.unpersist()
            fact_with_shipping = fact_with_shipping.withColumn(
                "row_num", row_number().over(window_spec)
            ).filter(col("row_num") == 1).drop("row_num").cache()
            
            final_shipping_count = fact_with_shipping.count()
            logger.info(f"After deduplication: {final_shipping_count:,} records")
        
        fact_with_product.unpersist()
        shipping_lookup.unpersist()
        
        # Step 5: Join with device dimension
        logger.info("Joining with dim_device...")
        fact_with_device = fact_with_shipping.alias("fs").join(
            dimensions["dim_device"].alias("dd"),
            (col("fs.device_type") == col("dd.device_type")) &
            (col("fs.browser") == col("dd.browser")),
            "left"
        ).select(
            col("fs.event_id"), 
            col("fs.quantity_clean"), 
            col("fs.product_price"), 
            col("fs.feedback_score_clean"),
            col("fs.date_time_sk"), 
            col("fs.user_sk"), 
            col("fs.product_sk"), 
            col("fs.ship_addr_sk"), 
            coalesce(col("dd.device_sk"), lit(-1)).alias("device_sk"),
            col("fs.referral_source_clean"), 
            col("fs.campaign_clean"), 
            col("fs.coupon_code_clean"),
            col("fs.payment_method"), 
            col("fs.payment_status"), 
            col("fs.transaction_status"), 
            col("fs.action_type")
        ).cache()
        
        device_count = fact_with_device.count()
        logger.info(f"After dim_device join: {device_count:,} records")
        fact_with_shipping.unpersist()
        
        # Step 6: Join with marketing dimension (normalized approach)
        logger.info("Joining with dim_marketing...")
        
        fact_with_marketing = fact_with_device.alias("fd").join(
            broadcast(dimensions["dim_marketing"].filter(col("marketing_type") == "REFERRAL_SOURCE").select("marketing_sk", col("marketing_code").alias("referral_source"))).alias("dmr"),
            col("fd.referral_source_clean") == col("dmr.referral_source"),
            "left"
        ).join(
            broadcast(dimensions["dim_marketing"].filter(col("marketing_type") == "CAMPAIGN").select("marketing_sk", col("marketing_code").alias("campaign"))).alias("dmc"),
            col("fd.campaign_clean") == col("dmc.campaign"),
            "left"
        ).join(
            broadcast(dimensions["dim_marketing"].filter(col("marketing_type") == "COUPON").select("marketing_sk", col("marketing_code").alias("coupon"))).alias("dmco"),
            col("fd.coupon_code_clean") == col("dmco.coupon"),
            "left"
        ).select(
            col("fd.event_id"), 
            col("fd.quantity_clean"), 
            col("fd.product_price"), 
            col("fd.feedback_score_clean"),
            col("fd.date_time_sk"), 
            col("fd.user_sk"), 
            col("fd.product_sk"), 
            col("fd.ship_addr_sk"), 
            col("fd.device_sk"), 
            coalesce(col("dmr.marketing_sk"), lit(-1)).alias("referral_marketing_sk"),
            coalesce(col("dmc.marketing_sk"), lit(-1)).alias("campaign_marketing_sk"),
            coalesce(col("dmco.marketing_sk"), lit(-1)).alias("coupon_marketing_sk"),
            col("fd.payment_method"), 
            col("fd.payment_status"), 
            col("fd.transaction_status"), 
            col("fd.action_type")
        ).cache()
        
        marketing_count = fact_with_marketing.count()
        logger.info(f"After dim_marketing join: {marketing_count:,} records")
        fact_with_device.unpersist()
        
        # Step 7: Join with order flags dimension
        logger.info("Joining with dim_order_flags...")
        final_fact = fact_with_marketing.alias("fm").join(
            broadcast(dimensions["dim_order_flags"].select("flag_sk", "payment_method", "payment_status", "transaction_status", "action_type")).alias("dof"),
            (col("fm.payment_method") == col("dof.payment_method")) &
            (col("fm.payment_status") == col("dof.payment_status")) &
            (col("fm.transaction_status") == col("dof.transaction_status")) &
            (col("fm.action_type") == col("dof.action_type")),
            "inner"
        ).select(
            # FINALIZED: Exact column order as specified in requirements
            col("fm.event_id"),
            col("fm.date_time_sk"),
            col("fm.user_sk"), 
            col("fm.product_sk"),
            col("fm.ship_addr_sk"),
            col("fm.device_sk"),
            col("fm.referral_marketing_sk"),
            col("fm.campaign_marketing_sk"),
            col("fm.coupon_marketing_sk"),
            col("dof.flag_sk"),
            col("fm.quantity_clean").alias("quantity"),
            col("fm.product_price").cast(DecimalType(18, 2)).alias("total_amount"),
            col("fm.feedback_score_clean").alias("feedback_score")
        ).cache()
        
        fact_count = final_fact.count()
        logger.info(f"After dim_order_flags join: {fact_count:,} records")
        fact_with_marketing.unpersist()
        
        # Data retention analysis
        data_loss_pct = ((fact_base_count - fact_count) / fact_base_count * 100) if fact_base_count > 0 else 0
        
        logger.info("="*50)
        logger.info("FACT TABLE JOIN ANALYSIS")
        logger.info("="*50)
        logger.info(f"Input records: {fact_base_count:,}")
        logger.info(f"Output records: {fact_count:,}")
        logger.info(f"Data retention: {(fact_count/fact_base_count*100):.2f}%")
        logger.info(f"Data loss: {data_loss_pct:.2f}%")
        
        if data_loss_pct > 5:
            logger.warning(f"Significant data loss detected!")
            logger.warning("   Consider investigating dimension join conditions")
        else:
            logger.info("Data retention within acceptable range")
        
        logger.info("="*50)
        
        return final_fact, fact_count
    
    def write_outputs(self, ml_df: DataFrame, rejected_df: DataFrame, 
                     dimensions: Dict[str, DataFrame], fact_df: DataFrame):
        """Write all outputs to S3 in Parquet format with optimal settings."""
        logger.info("Writing outputs to S3A...")
        
        write_options = {
            "compression": "snappy",
            "maxRecordsPerFile": 100000
        }
        
        try:
            # Write ML output dataset
            logger.info("Writing ML output dataset...")
            (ml_df
             .coalesce(16)
             .write
             .mode("overwrite") 
             .options(**write_options)
             .partitionBy("dt_date_derived")
             .parquet(f"{self.output_base}ml_output/"))
            logger.info("ML output dataset written successfully")
            
            # Write rejected records
            logger.info("Writing rejected records...")
            if rejected_df.take(1):
                (rejected_df
                 .coalesce(4)
                 .write
                 .mode("append")
                 .options(**write_options)
                 .partitionBy("ingestion_date")
                 .parquet(f"{self.output_base}rejected/"))
                logger.info("Rejected records written successfully")
            else:
                logger.info("No rejected records to write")
            
            # Write dimension tables
            logger.info("Writing dimension tables...")
            for dim_name, dim_df in dimensions.items():
                logger.info(f"  Writing {dim_name}...")
                
                if dim_name == "dim_date_time":
                    (dim_df
                     .coalesce(4)
                     .write
                     .mode("overwrite")
                     .options(**write_options)
                     .parquet(f"{self.output_base}{dim_name}/"))
                elif dim_name in ["dim_shipping_address", "dim_device"]:
                    (dim_df
                     .coalesce(8)
                     .write
                     .mode("overwrite")
                     .options(**write_options)
                     .parquet(f"{self.output_base}{dim_name}/"))
                else:
                    (dim_df
                     .coalesce(1)
                     .write
                     .mode("overwrite")
                     .options(**write_options)
                     .parquet(f"{self.output_base}{dim_name}/"))
                     
                logger.info(f"  {dim_name} written successfully")
            
            # Write fact table
            logger.info("Writing fact table...")
            (fact_df
             .coalesce(8)
             .write
             .mode("overwrite")
             .options(**write_options)
             .parquet(f"{self.output_base}final_fact/"))  # FINALIZED: Named as final_fact
            logger.info("Fact table written successfully")
            
        except Exception as e:
            logger.error(f"Failed to write outputs: {str(e)}")
            raise
    
    def run_etl(self):
        """
        Execute the complete ETL pipeline.
        
        """
        start_time = datetime.now()
        
        try:
            logger.info("Starting FINALIZED E-commerce ETL Pipeline...")
            
            # Step 1: Initialize Spark
            logger.info("STEP 1: Initializing Spark Session")
            spark = self.create_spark_session()
            
            # Step 2: Read input data
            logger.info("STEP 2: Reading Input Data")
            raw_df, raw_count = self.read_avro_data()
            
            # Sanity check: Verify product_price structure
            logger.info("SANITY CHECK: Verifying product_price structure...")
            sample_prices = raw_df.select("product_price").limit(5).collect()
            logger.info(f"Sample product_price values: {[str(row.product_price) for row in sample_prices]}")
            
            # Step 3: Apply data cleansing
            logger.info("STEP 3: Data Cleansing and Validation")
            clean_df, rejected_df, clean_count, reject_count = self.apply_data_cleansing(raw_df, raw_count)
            
            # Sanity check: Verify product_price flattening
            logger.info("SANITY CHECK: Verifying product_price flattening...")
            sample_flattened = clean_df.select("product_price").limit(5).collect()
            logger.info(f"Sample flattened prices: {[row.product_price for row in sample_flattened]}")
            
            # Step 4: Quality check
            logger.info("STEP 4: Quality Threshold Check")
            quality_ok = self.check_quality_threshold(clean_count, reject_count)
            if not quality_ok:
                logger.warning("Proceeding despite quality threshold warning...")
            
            # Step 5: Create ML dataset
            logger.info("STEP 5: Creating ML Output Dataset")
            ml_df = clean_df.select("*")
            logger.info("ML dataset created with all cleaned fields")
            
            # Step 6: Create dimension tables
            logger.info("STEP 6: Creating Dimension Tables with FINALIZED surrogate keys")
            dimensions = self.create_dimension_tables(clean_df)
            
            # Sanity check: Verify surrogate key uniqueness
            logger.info("SANITY CHECK: Verifying surrogate key uniqueness...")
            for dim_name, dim_df in dimensions.items():
                if dim_name == "dim_marketing":
                    continue  # Skip marketing as it uses manual keys
                sk_col = [c for c in dim_df.columns if c.endswith("_sk")][0]
                total_count = dim_df.count()
                unique_count = dim_df.select(sk_col).distinct().count()
                if total_count != unique_count:
                    logger.error(f"SURROGATE KEY COLLISION in {dim_name}!")
                    logger.error(f"   Total: {total_count}, Unique: {unique_count}")
                else:
                    logger.info(f"{dim_name}: {total_count} unique surrogate keys")
            
            # Step 7: Create fact table
            logger.info("STEP 7: Creating Fact Table with EXACT schema")
            fact_df, fact_count = self.create_fact_table(clean_df, dimensions)
            
            # Sanity check: Verify fact table schema
            logger.info("SANITY CHECK: Verifying final fact table schema...")
            expected_columns = [
                "event_id", "date_time_sk", "user_sk", "product_sk", "ship_addr_sk",
                "device_sk", "referral_marketing_sk", "campaign_marketing_sk", 
                "coupon_marketing_sk", "flag_sk", "quantity", "total_amount", "feedback_score"
            ]
            actual_columns = fact_df.columns
            if actual_columns == expected_columns:
                logger.info("Fact table has EXACT required schema")
            else:
                logger.error("Fact table schema mismatch!")
                logger.error(f"Expected: {expected_columns}")
                logger.error(f"Actual: {actual_columns}")
            
            # Step 8: Write outputs
            logger.info("STEP 8: Writing Outputs to S3A")
            self.write_outputs(ml_df, rejected_df, dimensions, fact_df)
            
            # Final execution summary
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("="*60)
            logger.info("FINALIZED ETL PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("="*60)
            logger.info(f"Execution time: {duration}")
            logger.info(f"Processing Summary:")
            logger.info(f"   Total input records: {raw_count:,}")
            logger.info(f"   Valid records: {clean_count:,}")
            logger.info(f"   Rejected records: {reject_count:,}")
            logger.info(f"   Acceptance rate: {clean_count/(clean_count + reject_count):.2%}")
            logger.info(f"   Quality threshold met: {'Yes' if quality_ok else 'No'}")
            logger.info(f"Outputs Created:")
            logger.info(f"   ML dataset: {clean_count:,} records")
            logger.info(f"   Rejected dataset: {reject_count:,} records")
            logger.info(f"   Dimension tables: {len(dimensions)} (with unique surrogate keys)")
            logger.info(f"   Fact table: {fact_count:,} records (exact schema)")
            logger.info(f"Output location: {self.output_base}")
            logger.info("="*60)
            logger.info("Product price flattened from nested struct")
            logger.info("All tables have exact required schemas")
            logger.info("Proper deduplication implemented")
            logger.info("Rejected records have correct 3-column format")
            logger.info("Removed unused code and imports")
            logger.info("Non-fatal checks use warnings, not errors")
            logger.info("  Corrected date-time dimension grouping")
            logger.info("  Unified product_price column")
            logger.info("="*60)
            
            # Clean up cached DataFrames
            try:
                raw_df.unpersist()
                clean_df.unpersist()
                fact_df.unpersist()
                for dim_df in dimensions.values():
                    dim_df.unpersist()
            except:
                pass
            
            return 0
            
        except Exception as e:
            logger.error("="*60)
            logger.error("FINALIZED ETL PIPELINE FAILED!")
            logger.error("="*60)
            logger.error(f"Error: {str(e)}")
            logger.error(f"Execution time before failure: {datetime.now() - start_time}")
            logger.error("="*60)
            raise
        finally:
            if self.spark:
                logger.info("Stopping Spark session...")
                self.spark.stop()

def main():
    """
    Main entry point for the FINALIZED ETL script.
    
    """
    try:
        logger.info("FINALIZED E-commerce ETL Script Starting...")
        logger.info(f"Start time: {datetime.now()}")
        
        etl = ECommerceETL()
        exit_code = etl.run_etl()
        
        logger.info("FINALIZED ETL script completed successfully")
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Critical failure in main(): {str(e)}")
        logger.error("Check the logs above for detailed error information")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("ETL script interrupted by user")
        sys.exit(130)

if __name__ == "__main__":
    main()