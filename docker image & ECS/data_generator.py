#!/usr/bin/env python3
"""
Memory-Optimized Real-time E-commerce Data Generator with ECS Fargate Compatible Logging
======================================================================================

Optimized for memory efficiency while maintaining data realism and business logic
UPDATED: Fixed shipping address constraint for purchase transactions
ENHANCED: Improved shipping address assignment logic for better consistency
"""

import json
import uuid
import time
import random
import signal
import sys
import traceback
import logging
import os
import gc
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Tuple, Generator
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from faker import Faker

# Modern Confluent Kafka with Avro support
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# ECS Fargate Compatible Logging Configuration - FIXED
def setup_ecs_logging():
    """
    Setup logging configuration that works properly in ECS Fargate environment.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    
    # Get log level from environment variable (default to INFO)
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    
    # Create formatter for ECS Fargate
    formatter = logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler that outputs to stdout (ECS Fargate requirement)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level))
    console_handler.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(console_handler)
    
    # Set specific logger levels
    logging.getLogger('confluent_kafka').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    # Force flush stdout/stderr
    sys.stdout.flush()
    sys.stderr.flush()
    
    return logging.getLogger(__name__)

# Initialize ECS compatible logger
logger = setup_ecs_logging()

# Configuration Constants - UPDATED WITH CORRECT ELASTIC IP
SCHEMA_REGISTRY_URL = "http://34.246.13.35:8081"
KAFKA_BROKER = "34.246.13.35:9092"
KAFKA_TOPIC = "ecommerce-events"
RECORDS_PER_BATCH = 10000
BATCH_INTERVAL_SECONDS = 5
SCRIPT_DURATION_SECONDS = 1140
CORRUPTION_PROBABILITY = 1 / 1000
MAX_RETRIES = 10
RETRY_BACKOFF_BASE = 2
HEALTH_CHECK_INTERVAL = 30  # seconds

@dataclass
class GeographicProfile:
    """Geographic profile for maintaining geo-spatial consistency across user interactions."""
    country: str
    cities: List[str]
    currency: str
    language: str
    timezone: str
    phone_prefix: str

class ModernAvroDataGenerator:
    """
    Memory-optimized e-commerce data generator with ECS Fargate compatible logging.
    
    This class generates realistic e-commerce transaction data with:
    - 350,000 unique users with consistent profiles
    - 10,000 products across multiple categories
    - 40+ countries with realistic geographic distribution
    - Seasonal and hourly shopping patterns
    - Marketing campaigns and referral tracking
    - FIXED: Guaranteed valid shipping addresses for purchase transactions
    - ENHANCED: Users limited to 1-2 consistent shipping addresses
    """
    
    def __init__(self):
        """Initialize the memory-optimized data generator with efficient configurations."""
        # Enhanced user pool to 350,000 users
        self.user_pool_size = 350_000
        # Enhanced product catalog to 10,000 products
        self.product_catalog_size = 10_000
        
        # MEMORY OPTIMIZATION: Use more efficient data structures
        self.seen_users = set()
        self.user_first_purchase_dates = {}  # Track first purchase date for each user
        # MEMORY OPTIMIZATION: Limit cached user data to recent users only
        self.user_profiles_cache = {}  # Limited cache for user profiles
        self.user_addresses_cache = {}  # Limited cache for user addresses
        self.max_cache_size = 350000  # Limit cache to prevent memory bloat
        
        # MEMORY OPTIMIZATION: Create lightweight references instead of full objects
        self.geographic_profiles = self._initialize_lightweight_geographic_profiles()
        self.product_templates = self._initialize_product_templates()  # Templates instead of full catalog
        self.marketing_campaigns = self._initialize_lightweight_marketing_data()
        self.country_product_preferences = self._initialize_country_product_preferences()
        
        # MEMORY OPTIMIZATION: Use single Faker instance
        self.faker = Faker()
        Faker.seed(42)  # Consistent seed for reproducibility
        
        # Enhanced temporal behavior patterns - keep as lightweight
        self.seasonal_multipliers = self._initialize_seasonal_patterns()
        self.hourly_patterns = self._initialize_hourly_patterns()
        
        # Date range: from January 1, 2024 to current date
        self.start_date = datetime(2024, 1, 1)
        self.end_date = datetime.now()
        
        # Force immediate log output
        sys.stdout.flush()
        logger.info("🚀 Memory-Optimized ECS Fargate Data Generator initializing...")
        sys.stdout.flush()
        
        # Initialize Schema Registry and Serializers - GLOBAL INITIALIZATION
        self.schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self.value_schema_str, self.key_schema_str = self._load_avro_schemas()
        
        if not self.value_schema_str:
            logger.error("❌ Avro value schema is required for Modern Kafka Avro serialization")
            sys.stdout.flush()
            sys.exit(1)
        
        # Create serializers - REUSE ACROSS BATCHES
        self.avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=self.value_schema_str
        )
        self.string_serializer = StringSerializer('utf_8')
        
        # Performance and health tracking
        self.total_sent = 0
        self.total_failed = 0
        self.batch_count = 0
        self.start_time = time.time()
        self.last_successful_send = time.time()
        self.consecutive_failures = 0
        self.max_consecutive_failures = 5
        
        # Connection health
        self.producer = None
        self.producer_created_at = None
        self.producer_max_age = 3600  # 1 hour
        
        # Graceful shutdown handling
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.shutdown_requested = False
        
        logger.info("🚀 Memory-Optimized ECS Data Generator initialized successfully")
        logger.info(f"📊 Configuration: user_pool={self.user_pool_size:,}, products={self.product_catalog_size:,}, countries={len(self.geographic_profiles)}, batch_size={RECORDS_PER_BATCH:,}")
        sys.stdout.flush()
    
    def _initialize_lightweight_geographic_profiles(self) -> List[GeographicProfile]:
        """
        Initialize lightweight geographic profiles with minimal memory footprint.
        
        Returns:
            List[GeographicProfile]: List of geographic profiles covering major markets
        """
        # MEMORY OPTIMIZATION: Keep only essential data
        return [
            # North America
            GeographicProfile("United States", ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"], "USD", "en", "America/New_York", "+1"),
            GeographicProfile("Canada", ["Toronto", "Montreal", "Vancouver", "Calgary"], "CAD", "en", "America/Toronto", "+1"),
            GeographicProfile("Mexico", ["Mexico City", "Guadalajara", "Monterrey"], "MXN", "es", "America/Mexico_City", "+52"),
            
            # Europe
            GeographicProfile("United Kingdom", ["London", "Manchester", "Birmingham", "Leeds"], "GBP", "en", "Europe/London", "+44"),
            GeographicProfile("Germany", ["Berlin", "Munich", "Hamburg", "Cologne"], "EUR", "de", "Europe/Berlin", "+49"),
            GeographicProfile("France", ["Paris", "Lyon", "Marseille", "Toulouse"], "EUR", "fr", "Europe/Paris", "+33"),
            GeographicProfile("Spain", ["Madrid", "Barcelona", "Valencia", "Seville"], "EUR", "es", "Europe/Madrid", "+34"),
            GeographicProfile("Italy", ["Rome", "Milan", "Naples", "Turin"], "EUR", "it", "Europe/Rome", "+39"),
            GeographicProfile("Netherlands", ["Amsterdam", "Rotterdam", "The Hague"], "EUR", "nl", "Europe/Amsterdam", "+31"),
            GeographicProfile("Sweden", ["Stockholm", "Gothenburg", "Malmö"], "SEK", "sv", "Europe/Stockholm", "+46"),
            GeographicProfile("Norway", ["Oslo", "Bergen", "Trondheim"], "NOK", "no", "Europe/Oslo", "+47"),
            GeographicProfile("Denmark", ["Copenhagen", "Aarhus", "Odense"], "DKK", "da", "Europe/Copenhagen", "+45"),
            GeographicProfile("Switzerland", ["Zurich", "Geneva", "Basel"], "CHF", "de", "Europe/Zurich", "+41"),
            GeographicProfile("Austria", ["Vienna", "Salzburg", "Innsbruck"], "EUR", "de", "Europe/Vienna", "+43"),
            GeographicProfile("Belgium", ["Brussels", "Antwerp", "Ghent"], "EUR", "nl", "Europe/Brussels", "+32"),
            GeographicProfile("Poland", ["Warsaw", "Krakow", "Gdansk"], "PLN", "pl", "Europe/Warsaw", "+48"),
            GeographicProfile("Portugal", ["Lisbon", "Porto", "Braga"], "EUR", "pt", "Europe/Lisbon", "+351"),
            
            # Asia-Pacific
            GeographicProfile("Japan", ["Tokyo", "Osaka", "Yokohama", "Nagoya"], "JPY", "ja", "Asia/Tokyo", "+81"),
            GeographicProfile("South Korea", ["Seoul", "Busan", "Incheon"], "KRW", "ko", "Asia/Seoul", "+82"),
            GeographicProfile("China", ["Beijing", "Shanghai", "Guangzhou", "Shenzhen"], "CNY", "zh", "Asia/Shanghai", "+86"),
            GeographicProfile("Australia", ["Sydney", "Melbourne", "Brisbane", "Perth"], "AUD", "en", "Australia/Sydney", "+61"),
            GeographicProfile("New Zealand", ["Auckland", "Wellington", "Christchurch"], "NZD", "en", "Pacific/Auckland", "+64"),
            GeographicProfile("Singapore", ["Singapore"], "SGD", "en", "Asia/Singapore", "+65"),
            GeographicProfile("India", ["Mumbai", "Delhi", "Bangalore", "Hyderabad"], "INR", "en", "Asia/Kolkata", "+91"),
            GeographicProfile("Thailand", ["Bangkok", "Chiang Mai", "Phuket"], "THB", "th", "Asia/Bangkok", "+66"),
            GeographicProfile("Malaysia", ["Kuala Lumpur", "George Town", "Johor Bahru"], "MYR", "en", "Asia/Kuala_Lumpur", "+60"),
            GeographicProfile("Indonesia", ["Jakarta", "Surabaya", "Bandung"], "IDR", "id", "Asia/Jakarta", "+62"),
            GeographicProfile("Philippines", ["Manila", "Quezon City", "Davao"], "PHP", "en", "Asia/Manila", "+63"),
            GeographicProfile("Vietnam", ["Ho Chi Minh City", "Hanoi", "Da Nang"], "VND", "vi", "Asia/Ho_Chi_Minh", "+84"),
            
            # Middle East & Africa
            GeographicProfile("Egypt", ["Cairo", "Alexandria", "Giza", "Port Said"], "EGP", "ar", "Africa/Cairo", "+20"),
            GeographicProfile("Saudi Arabia", ["Riyadh", "Jeddah", "Mecca", "Medina"], "SAR", "ar", "Asia/Riyadh", "+966"),
            GeographicProfile("UAE", ["Dubai", "Abu Dhabi", "Sharjah"], "AED", "ar", "Asia/Dubai", "+971"),
            GeographicProfile("Turkey", ["Istanbul", "Ankara", "Izmir"], "TRY", "tr", "Europe/Istanbul", "+90"),
            GeographicProfile("Israel", ["Tel Aviv", "Jerusalem", "Haifa"], "ILS", "he", "Asia/Jerusalem", "+972"),
            GeographicProfile("Jordan", ["Amman", "Zarqa", "Irbid"], "JOD", "ar", "Asia/Amman", "+962"),
            GeographicProfile("Lebanon", ["Beirut", "Tripoli", "Sidon"], "LBP", "ar", "Asia/Beirut", "+961"),
            GeographicProfile("South Africa", ["Cape Town", "Johannesburg", "Durban"], "ZAR", "en", "Africa/Johannesburg", "+27"),
            GeographicProfile("Nigeria", ["Lagos", "Kano", "Ibadan"], "NGN", "en", "Africa/Lagos", "+234"),
            GeographicProfile("Kenya", ["Nairobi", "Mombasa", "Kisumu"], "KES", "en", "Africa/Nairobi", "+254"),
            
            # South America
            GeographicProfile("Brazil", ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador"], "BRL", "pt", "America/Sao_Paulo", "+55"),
            GeographicProfile("Argentina", ["Buenos Aires", "Córdoba", "Rosario"], "ARS", "es", "America/Argentina/Buenos_Aires", "+54"),
            GeographicProfile("Chile", ["Santiago", "Valparaíso", "Concepción"], "CLP", "es", "America/Santiago", "+56"),
            GeographicProfile("Colombia", ["Bogotá", "Medellín", "Cali"], "COP", "es", "America/Bogota", "+57"),
            GeographicProfile("Peru", ["Lima", "Arequipa", "Trujillo"], "PEN", "es", "America/Lima", "+51"),
        ]
    
    def _initialize_product_templates(self) -> Dict:
        """
        Initialize lightweight product templates instead of full catalog to save memory.
        
        Returns:
            Dict: Product templates with generation parameters
        """
        # MEMORY OPTIMIZATION: Store templates instead of full product catalog
        return {
            "Electronics": {
                "brands": ["Apple", "Samsung", "Sony", "LG", "Huawei", "Xiaomi", "OnePlus", "Google", "Microsoft", "Dell", "HP", "Lenovo", "ASUS"],
                "price_range": (29.99, 2999.99),
                "products": ["iPhone", "Galaxy Phone", "MacBook", "iPad", "Smart TV", "Laptop", "Desktop", "Headphones", "Speaker", "Camera", "Monitor", "Keyboard", "Mouse", "Tablet"]
            },
            "Fashion": {
                "brands": ["Nike", "Adidas", "H&M", "Zara", "Uniqlo", "Gap", "Levi's", "Calvin Klein", "Tommy Hilfiger", "Ralph Lauren"],
                "price_range": (9.99, 899.99),
                "products": ["T-Shirt", "Jeans", "Sneakers", "Dress", "Jacket", "Sweater", "Hoodie", "Pants", "Skirt", "Blouse", "Coat", "Boots", "Sandals", "Hat"]
            },
            "Home & Garden": {
                "brands": ["IKEA", "Wayfair", "West Elm", "Ashley", "Pottery Barn", "Crate & Barrel", "Home Depot", "Lowe's"],
                "price_range": (4.99, 1999.99),
                "products": ["Sofa", "Chair", "Table", "Bed", "Mattress", "Lamp", "Rug", "Curtains", "Mirror", "Vase", "Plant", "Cushion", "Blanket", "Frame"]
            },
            "Books": {
                "brands": ["Penguin", "Random House", "HarperCollins", "Simon & Schuster", "Macmillan", "Hachette"],
                "price_range": (5.99, 89.99),
                "products": ["Novel", "Textbook", "Biography", "Cookbook", "Self-Help", "History", "Science", "Fiction", "Romance", "Mystery", "Thriller", "Poetry"]
            },
            "Sports & Outdoors": {
                "brands": ["Nike", "Adidas", "Under Armour", "Puma", "Reebok", "New Balance", "Columbia", "North Face", "Patagonia"],
                "price_range": (12.99, 599.99),
                "products": ["Running Shoes", "Yoga Mat", "Dumbbells", "Bicycle", "Backpack", "Tent", "Sleeping Bag", "Water Bottle", "Fitness Tracker", "Golf Club"]
            },
            "Beauty & Personal Care": {
                "brands": ["L'Oréal", "MAC", "Sephora", "Clinique", "Estée Lauder", "Maybelline", "Revlon", "CoverGirl"],
                "price_range": (3.99, 199.99),
                "products": ["Lipstick", "Foundation", "Mascara", "Perfume", "Moisturizer", "Shampoo", "Conditioner", "Face Mask", "Sunscreen", "Body Lotion"]
            },
            "Toys & Games": {
                "brands": ["LEGO", "Mattel", "Hasbro", "Fisher-Price", "Playmobil", "Bandai", "Funko"],
                "price_range": (7.99, 299.99),
                "products": ["Building Set", "Action Figure", "Doll", "Board Game", "Puzzle", "Remote Control Car", "Video Game", "Educational Toy", "Stuffed Animal"]
            },
            "Health & Wellness": {
                "brands": ["Nature Made", "Centrum", "Optimum Nutrition", "Garden of Life", "NOW Foods", "Thorne"],
                "price_range": (8.99, 129.99),
                "products": ["Multivitamin", "Protein Powder", "Fish Oil", "Probiotics", "Vitamin D", "Calcium", "Magnesium", "CoQ10", "Turmeric", "Omega-3"]
            },
            "Automotive": {
                "brands": ["Bosch", "Michelin", "Goodyear", "Castrol", "Mobil 1", "K&N", "NGK", "Brembo"],
                "price_range": (15.99, 899.99),
                "products": ["Motor Oil", "Air Filter", "Brake Pads", "Spark Plugs", "Car Battery", "Tire", "Wiper Blades", "Floor Mats", "Phone Mount"]
            },
            "Pet Supplies": {
                "brands": ["Hill's", "Royal Canin", "Purina", "Blue Buffalo", "Wellness", "KONG"],
                "price_range": (4.99, 199.99),
                "products": ["Dog Food", "Cat Food", "Pet Toy", "Leash", "Pet Bed", "Litter Box", "Food Bowl", "Collar", "Pet Shampoo", "Treats"]
            }
        }
    
    def _initialize_lightweight_marketing_data(self) -> Dict:
        """
        Initialize lightweight marketing campaigns and referral sources.
        
        Returns:
            Dict: Dictionary containing referral sources, campaigns, and coupon codes
        """
        # MEMORY OPTIMIZATION: Keep only essential marketing data
        return {
            "referral_sources": ["google", "facebook", "instagram", "twitter", "tiktok", "youtube", "email", "direct", "bing"],
            "campaigns": ["summer_sale_2024", "black_friday_2024", "cyber_monday", "new_year_2025", "flash_sale", "clearance"],
            "coupon_codes": ["SAVE10", "WELCOME20", "FLASH15", "LOYALTY25", "STUDENT10", "FIRST15"]
        }
    
    def _initialize_country_product_preferences(self) -> Dict:
        """
        Initialize country-specific product preferences.
        
        Returns:
            Dict: Dictionary mapping countries to product category preferences
        """
        return {
            "United States": {"Electronics": 0.25, "Fashion": 0.20, "Home & Garden": 0.15, "Books": 0.10, "Sports & Outdoors": 0.15, "Beauty & Personal Care": 0.10, "Toys & Games": 0.05},
            "Japan": {"Electronics": 0.35, "Fashion": 0.15, "Beauty & Personal Care": 0.15, "Books": 0.10, "Toys & Games": 0.10, "Home & Garden": 0.10, "Health & Wellness": 0.05},
            "Germany": {"Electronics": 0.20, "Automotive": 0.20, "Home & Garden": 0.15, "Books": 0.15, "Fashion": 0.15, "Health & Wellness": 0.10, "Sports & Outdoors": 0.05},
            "Saudi Arabia": {"Electronics": 0.20, "Fashion": 0.25, "Beauty & Personal Care": 0.15, "Home & Garden": 0.15, "Automotive": 0.10, "Books": 0.10, "Health & Wellness": 0.05},
            "UAE": {"Electronics": 0.22, "Fashion": 0.28, "Beauty & Personal Care": 0.18, "Home & Garden": 0.12, "Automotive": 0.10, "Books": 0.06, "Health & Wellness": 0.04},
            # Default distribution for other countries
            "default": {"Electronics": 0.20, "Fashion": 0.18, "Home & Garden": 0.15, "Books": 0.12, "Sports & Outdoors": 0.12, "Beauty & Personal Care": 0.12, "Toys & Games": 0.06, "Health & Wellness": 0.05}
        }
    
    def _initialize_seasonal_patterns(self) -> Dict:
        """Initialize seasonal shopping patterns."""
        return {
            1: 0.8, 2: 0.9, 3: 1.0, 4: 1.1, 5: 1.2, 6: 1.1,
            7: 1.0, 8: 1.1, 9: 1.2, 10: 1.3, 11: 1.6, 12: 1.8
        }
    
    def _initialize_hourly_patterns(self) -> Dict:
        """Initialize hourly shopping patterns."""
        return {
            0: 0.3, 1: 0.2, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.2,
            6: 0.4, 7: 0.6, 8: 0.8, 9: 1.0, 10: 1.2, 11: 1.3,
            12: 1.4, 13: 1.3, 14: 1.2, 15: 1.1, 16: 1.0, 17: 0.9,
            18: 1.1, 19: 1.3, 20: 1.5, 21: 1.4, 22: 1.2, 23: 0.8
        }
    
    def _generate_product_on_demand(self, country: str, product_id: int) -> Dict:
        """
        Generate product data on-demand instead of storing full catalog.
        
        Args:
            country (str): Country name for preference weighting
            product_id (int): Product ID
            
        Returns:
            Dict: Generated product data
        """
        # MEMORY OPTIMIZATION: Generate products on-demand instead of storing catalog
        preferences = self.country_product_preferences.get(country, self.country_product_preferences["default"])
        
        # Select category based on country preferences
        categories = list(preferences.keys())
        weights = [preferences[cat] for cat in categories]
        category = random.choices(categories, weights=weights)[0]
        
        # Get template for category
        template = self.product_templates.get(category, self.product_templates["Electronics"])
        brand = random.choice(template["brands"])
        product_type = random.choice(template["products"])
        base_price = random.uniform(template["price_range"][0], template["price_range"][1])
        
        return {
            "product_id": product_id,
            "product_name": f"{brand} {product_type} {product_id}",
            "product_category": category,
            "product_brand": brand,
            "base_price": round(base_price, 2)
        }
    
    def _get_user_profile(self, user_id: str) -> Dict:
        """
        Get or create user profile with memory-efficient caching.
        
        Args:
            user_id (str): Unique user identifier
            
        Returns:
            Dict: User profile containing demographic and preference data
        """
        # MEMORY OPTIMIZATION: LRU-style cache management
        if user_id not in self.user_profiles_cache:
            # Clean cache if it's getting too large
            if len(self.user_profiles_cache) >= self.max_cache_size:
                # Remove oldest 20% of entries
                remove_count = self.max_cache_size // 5
                oldest_keys = list(self.user_profiles_cache.keys())[:remove_count]
                for key in oldest_keys:
                    del self.user_profiles_cache[key]
                    if key in self.user_addresses_cache:
                        del self.user_addresses_cache[key]
                
                # Force garbage collection
                gc.collect()
            
            # Assign user to a geographic profile
            geo_profile = random.choice(self.geographic_profiles)
            city = random.choice(geo_profile.cities)
            
            # Generate consistent user data
            self.user_profiles_cache[user_id] = {
                "name": self.faker.name(),
                "email": self.faker.email(),
                "phone": self._generate_phone_number(geo_profile),
                "geo_profile": geo_profile,
                "city": city
            }
        
        return self.user_profiles_cache[user_id]
    
    def _load_avro_schemas(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Load Avro schemas as strings for Modern Kafka integration.
        
        Returns:
            Tuple[Optional[str], Optional[str]]: Value schema and key schema strings
        """
        try:
            logger.info("📋 Loading Avro schemas...")
            sys.stdout.flush()
            
            # Load value schema
            value_schema_path = Path("transaction.avsc")
            if not value_schema_path.exists():
                logger.error("❌ Avro schema file 'transaction.avsc' not found!")
                logger.info("📝 Creating default Avro schema...")
                sys.stdout.flush()
                self._create_default_avro_schema()
                
            with open(value_schema_path, 'r') as f:
                value_schema_dict = json.load(f)
            
            value_schema_str = json.dumps(value_schema_dict)
            key_schema_str = '"string"'  # Simple string schema
            
            logger.info(f"✅ Avro schemas loaded successfully: {value_schema_dict.get('name', 'unknown')}")
            sys.stdout.flush()
            
            return value_schema_str, key_schema_str
            
        except Exception as e:
            logger.error(f"❌ Failed to load Avro schemas: {str(e)}")
            sys.stdout.flush()
            return None, None
    
    def _create_default_avro_schema(self):
        """Create a default Avro schema file if it doesn't exist."""
        default_schema = {
            "type": "record",
            "namespace": "com.ecommerce.events",
            "name": "Transaction",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "total_amount", "type": ["null", "double"], "default": None},
                {"name": "feedback_score", "type": ["null", "int"], "default": None},
                {"name": "user_id", "type": ["null", "string"], "default": None},
                {"name": "user_name", "type": "string"},
                {"name": "user_email", "type": "string"},
                {"name": "user_phone", "type": ["null", "string"], "default": None},
                {"name": "is_first_purchase", "type": "boolean", "default": False},
                {"name": "language", "type": "string"},
                {"name": "currency", "type": "string"},
                {"name": "city_name", "type": "string"},
                {"name": "country_name", "type": "string"},
                {"name": "product_id", "type": "int"},
                {"name": "product_name", "type": "string"},
                {"name": "product_category", "type": "string"},
                {"name": "product_brand", "type": "string"},
                {"name": "product_price", "type": ["string", "double"]},
                {"name": "shipping_address", "type": ["null", "string"], "default": None},
                {"name": "postal_code", "type": ["null", "string"], "default": None},
                {"name": "device_type", "type": "string"},
                {"name": "browser", "type": "string"},
                {"name": "referral_source", "type": ["null", "string"], "default": None},
                {"name": "campaign", "type": ["null", "string"], "default": None},
                {"name": "coupon_code", "type": ["null", "string"], "default": None},
                {"name": "coupon_code_flag", "type": ["null", "boolean"], "default": None},
                {"name": "payment_method", "type": "string"},
                {"name": "payment_status", "type": "string"},
                {"name": "transaction_status", "type": "string"},
                {"name": "action_type", "type": "string"},
                {"name": "event_timestamp", "type": ["null", "string"], "default": None},
                {"name": "dt_date", "type": "string"},
                {"name": "year", "type": "int"},
                {"name": "month", "type": "int"},
                {"name": "day", "type": "int"},
                {"name": "hour", "type": "int"}
            ]
        }
        
        try:
            with open("transaction.avsc", 'w') as f:
                json.dump(default_schema, f, indent=2)
            logger.info("✅ Default Avro schema created: transaction.avsc")
            sys.stdout.flush()
        except Exception as e:
            logger.error(f"❌ Failed to create default schema: {str(e)}")
            sys.stdout.flush()
    
    def _generate_valid_shipping_address(self, geo_profile: GeographicProfile, city: str) -> str:
        """
        Generate a valid shipping address for the given geographic profile and city.
        This ensures purchase transactions always have valid addresses.
        
        Args:
            geo_profile (GeographicProfile): Geographic profile for address format
            city (str): City name for the address
            
        Returns:
            str: Valid shipping address for the country/city
        """
        if geo_profile.country == "United States":
            street_names = ['Main St', 'Oak Ave', 'First St', 'Park Rd', 'Elm Street', 'Washington Ave']
            states = ['NY', 'CA', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
            address = f"{random.randint(100, 9999)} {random.choice(street_names)}, {city}, {random.choice(states)} {random.randint(10000, 99999)}"
        elif geo_profile.country == "United Kingdom":
            street_names = ['High Street', 'Church Lane', 'Victoria Road', 'King Street', 'Queen Street']
            postcodes = ['SW1A', 'E1', 'W1A', 'N1', 'SE1', 'EC1']
            address = f"{random.randint(1, 999)} {random.choice(street_names)}, {city} {random.choice(postcodes)} {random.randint(1, 9)}{random.choice(['AA', 'BB', 'CC'])}"
        elif geo_profile.country == "Egypt":
            street_names = ['شارع النيل', 'شارع التحرير', 'شارع الجمهورية', 'شارع أحمد عرابي', 'شارع طلعت حرب']
            address = f"{random.randint(1, 200)} {random.choice(street_names)}, {city}, {geo_profile.country}"
        elif geo_profile.country == "Germany":
            street_names = ['Hauptstraße', 'Bahnhofstraße', 'Kirchstraße', 'Schulstraße', 'Gartenstraße']
            address = f"{random.choice(street_names)} {random.randint(1, 200)}, {random.randint(10000, 99999)} {city}, Germany"
        elif geo_profile.country == "France":
            street_names = ['Rue de la Paix', 'Avenue des Champs', 'Rue Victor Hugo', 'Boulevard Saint-Germain', 'Rue de Rivoli']
            address = f"{random.randint(1, 200)} {random.choice(street_names)}, {random.randint(75000, 95999)} {city}, France"
        elif geo_profile.country in ["Saudi Arabia", "UAE"]:
            street_names = ['King Fahd Road', 'Prince Sultan Street', 'Olaya Street', 'King Abdul Aziz Road', 'Al Tahlia Street']
            address = f"{random.choice(street_names)}, {city} {random.randint(10000, 99999)}, {geo_profile.country}"
        elif geo_profile.country == "Canada":
            street_names = ['Main Street', 'King Street', 'Queen Street', 'First Avenue', 'Broadway']
            provinces = ['ON', 'BC', 'AB', 'QC', 'MB', 'SK', 'NS', 'NB']
            postal_code = f"{random.choice(['K', 'M', 'N', 'T', 'V'])}{random.randint(1, 9)}{random.choice(['A', 'B', 'C'])} {random.randint(1, 9)}{random.choice(['X', 'Y', 'Z'])}{random.randint(1, 9)}"
            address = f"{random.randint(100, 9999)} {random.choice(street_names)}, {city}, {random.choice(provinces)} {postal_code}, Canada"
        elif geo_profile.country == "Australia":
            street_names = ['Collins Street', 'George Street', 'Queen Street', 'Bourke Street', 'Elizabeth Street']
            states = ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']
            address = f"{random.randint(1, 999)} {random.choice(street_names)}, {city} {random.choice(states)} {random.randint(1000, 9999)}, Australia"
        elif geo_profile.country == "Japan":
            districts = ['Shibuya', 'Shinjuku', 'Ginza', 'Harajuku', 'Akihabara', 'Chiyoda']
            address = f"{random.randint(1, 50)}-{random.randint(1, 20)}-{random.randint(1, 99)} {random.choice(districts)}, {city}, Japan"
        elif geo_profile.country == "Brazil":
            street_names = ['Rua das Flores', 'Avenida Paulista', 'Rua Augusta', 'Rua Oscar Freire', 'Avenida Brasil']
            address = f"{random.choice(street_names)}, {random.randint(100, 9999)}, {city}, {random.randint(10000, 99999)}-{random.randint(100, 999)}, Brazil"
        else:
            # Generic international format
            street_names = ['Main Street', 'Central Avenue', 'Market Square', 'First Street', 'Broadway']
            address = f"{random.randint(1, 999)} {random.choice(street_names)}, {city}, {geo_profile.country}"
        
        return address
    
    def _get_user_shipping_address(self, user_id: str, user_profile: Dict, action_type: str) -> Optional[str]:
        """
        Get consistent shipping address for user with memory-efficient caching.
        UPDATED: Ensures purchase transactions always have valid addresses.
        ENHANCED: Better cache management and consistent address assignment.
        
        Args:
            user_id (str): Unique user identifier
            user_profile (Dict): User profile data
            action_type (str): Transaction action type
            
        Returns:
            Optional[str]: Shipping address or None (only for non-purchase transactions)
        """
        # CRITICAL FIX: If action_type is "purchase", ALWAYS return a valid address
        if action_type == "purchase":
            # For purchases, we MUST have a shipping address
            if user_id not in self.user_addresses_cache:
                # Create addresses for new user
                if len(self.user_addresses_cache) >= self.max_cache_size:
                    # Clean cache as before
                    remove_count = self.max_cache_size // 5
                    oldest_keys = list(self.user_addresses_cache.keys())[:remove_count]
                    for key in oldest_keys:
                        if key in self.user_addresses_cache:
                            del self.user_addresses_cache[key]
                    gc.collect()
                
                # Generate 1-2 valid addresses for the user
                addresses = []
                num_addresses = random.choices([1, 2], weights=[70, 30])[0]
                
                geo_profile = user_profile["geo_profile"]
                city = user_profile["city"]
                
                for _ in range(num_addresses):
                    # Always generate valid address for user's location
                    address = self._generate_valid_shipping_address(geo_profile, city)
                    addresses.append(address)
                
                self.user_addresses_cache[user_id] = addresses
            
            # Always return a valid address for purchases
            return random.choice(self.user_addresses_cache[user_id])
        
        else:
            # For non-purchase transactions (refund, return, cancellation), address can be null
            # MEMORY OPTIMIZATION: Same cache management as before
            if user_id not in self.user_addresses_cache:
                if len(self.user_addresses_cache) >= self.max_cache_size:
                    # Remove oldest 20% of entries
                    remove_count = self.max_cache_size // 5
                    oldest_keys = list(self.user_addresses_cache.keys())[:remove_count]
                    for key in oldest_keys:
                        if key in self.user_addresses_cache:
                            del self.user_addresses_cache[key]
                    gc.collect()
                
                # Create 1-2 addresses for new user
                addresses = []
                num_addresses = random.choices([1, 2], weights=[70, 30])[0]
                
                geo_profile = user_profile["geo_profile"]
                city = user_profile["city"]
                
                for _ in range(num_addresses):
                    address = self._generate_valid_shipping_address(geo_profile, city)
                    addresses.append(address)
                
                self.user_addresses_cache[user_id] = addresses
            
            # For non-purchase transactions, return address or None with some probability
            if random.random() < 0.15:  # 15% chance of None for non-purchases
                return None
            return random.choice(self.user_addresses_cache[user_id])
    
    def _get_realistic_timestamp(self) -> datetime:
        """
        Generate timestamp with seasonal and hourly patterns for realistic distribution.
        
        Returns:
            datetime: Timestamp following realistic e-commerce patterns
        """
        # First, get a random date in the range
        time_between = self.end_date - self.start_date
        days_between = time_between.days
        random_days = random.randint(0, days_between)
        base_date = self.start_date + timedelta(days=random_days)
        
        # Apply seasonal pattern
        month = base_date.month
        seasonal_multiplier = self.seasonal_multipliers[month]
        
        # Higher chance of generating dates in high-activity months
        if random.random() > seasonal_multiplier / 2.0:
            # Try again with bias toward high-activity months
            high_activity_months = [11, 12, 9, 10, 5]
            if random.random() < 0.3:
                target_month = random.choice(high_activity_months)
                if target_month <= self.end_date.month or base_date.year < self.end_date.year:
                    try:
                        base_date = base_date.replace(month=target_month)
                    except ValueError:
                        pass
        
        # Apply hourly pattern
        hourly_weights = list(self.hourly_patterns.values())
        hour = random.choices(range(24), weights=hourly_weights)[0]
        
        # Set the time with realistic patterns
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        return base_date.replace(hour=hour, minute=minute, second=second)
    
    def _test_schema_registry_connection(self) -> bool:
        """
        Test Schema Registry connection for health monitoring.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Test schema registry client
            subjects = self.schema_registry_client.get_subjects()
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema Registry connection failed: {str(e)}")
            sys.stdout.flush()
            return False
    
    def _create_kafka_producer(self) -> Producer:
        """
        Create Modern Kafka Producer with ECS Fargate compatible logging and health checks.
        
        Returns:
            Producer: Configured Kafka producer instance
        """
        if self.producer and self._check_producer_health():
            return self.producer
            
        if self.producer:
            try:
                self.producer.flush(timeout=5)
            except:
                pass
        
        max_retries = MAX_RETRIES
        
        for attempt in range(max_retries):
            try:
                if attempt == 0:
                    logger.info("🔄 Creating Kafka producer")
                    sys.stdout.flush()
                
                # Test Schema Registry connection first
                if not self._test_schema_registry_connection():
                    raise Exception("Cannot connect to Schema Registry")
                
                # Producer configuration
                producer_config = {
                    'bootstrap.servers': KAFKA_BROKER,
                    'acks': 1,
                    'retries': 3,
                    'batch.size': 16384,
                    'linger.ms': 10,
                    'compression.type': 'snappy',
                    'message.max.bytes': 5242880,  # 5MB
                    'request.timeout.ms': 30000,
                    'retry.backoff.ms': 1000,
                    'reconnect.backoff.ms': 2000,
                    'connections.max.idle.ms': 30000,
                    'enable.idempotence': False
                }
                
                # Create Kafka Producer
                producer = Producer(producer_config)
                
                # Test producer with a simple message
                try:
                    test_record = self._create_test_transaction_record()
                    serialization_context = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
                    
                    serialized_key = self.string_serializer("health_check")
                    serialized_value = self.avro_serializer(test_record, serialization_context)
                    
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=serialized_key,
                        value=serialized_value
                    )
                    producer.flush(timeout=10)
                    
                except Exception as test_error:
                    logger.warning(f"⚠️ Kafka Producer health check failed: {str(test_error)}")
                    sys.stdout.flush()
                
                self.producer = producer
                self.producer_created_at = time.time()
                self.consecutive_failures = 0
                logger.info("🎯 Kafka Producer created successfully")
                sys.stdout.flush()
                return producer
                
            except Exception as e:
                if attempt < max_retries - 1:
                    backoff = RETRY_BACKOFF_BASE ** attempt
                    time.sleep(backoff)
                else:
                    logger.error("🔴 Max retries exceeded for Kafka producer creation")
                    sys.stdout.flush()
                    raise
    
    def _check_producer_health(self) -> bool:
        """
        Check if producer needs to be recreated based on age and failure count.
        
        Returns:
            bool: True if producer is healthy, False if needs recreation
        """
        if self.producer is None:
            return False
            
        if (self.producer_created_at and 
            time.time() - self.producer_created_at > self.producer_max_age):
            return False
            
        if self.consecutive_failures >= self.max_consecutive_failures:
            return False
            
        return True
    
    def _create_test_transaction_record(self) -> Dict:
        """
        Create a test record for health checks and connection validation.
        
        Returns:
            Dict: Test transaction record
        """
        geo_profile = random.choice(self.geographic_profiles)
        city = random.choice(geo_profile.cities)
        product = self._generate_product_on_demand(geo_profile.country, 999999)
        current_time = self._get_realistic_timestamp()
        
        test_record = {
            "event_id": str(uuid.uuid4()),
            "quantity": 1,
            "total_amount": None,
            "feedback_score": None,
            "user_id": "health_check_user",
            "user_name": "Health Check User",
            "user_email": "healthcheck@example.com",
            "user_phone": None,
            "is_first_purchase": False,
            "language": geo_profile.language,
            "currency": geo_profile.currency,
            "city_name": city,
            "country_name": geo_profile.country,
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "product_category": product["product_category"],
            "product_brand": product["product_brand"],
            "product_price": product["base_price"],
            "shipping_address": self._generate_valid_shipping_address(geo_profile, city),  # Always valid for health check
            "postal_code": None,
            "device_type": "desktop",
            "browser": "Chrome",
            "referral_source": None,
            "campaign": None,
            "coupon_code": None,
            "coupon_code_flag": None,
            "payment_method": "health_check",
            "payment_status": "completed",
            "transaction_status": "success",
            "action_type": "health_check",
            "event_timestamp": current_time.isoformat() + "Z",
            "dt_date": current_time.strftime("%Y-%m-%d"),
            "year": current_time.year,
            "month": current_time.month,
            "day": current_time.day,
            "hour": current_time.hour
        }
        
        return test_record
    
    def generate_record(self) -> Dict:
        """
        Generate a single realistic enhanced transaction record with all business logic.
        UPDATED: Fixed shipping address constraint for purchase transactions.
        ENHANCED: Improved shipping address consistency per user.
        
        Returns:
            Dict: Complete transaction record with realistic data patterns
        """
        # Generate realistic timestamp with patterns
        current_time = self._get_realistic_timestamp()
        
        # Generate user ID and get/create user profile
        user_id_num = random.randint(1, self.user_pool_size)
        user_id = f"user_{user_id_num:06d}" if random.random() > 0.01 else None
        
        # Get user profile (consistent across purchases)
        if user_id:
            user_profile = self._get_user_profile(user_id)
            geo_profile = user_profile["geo_profile"]
            city = user_profile["city"]
            user_name = user_profile["name"]
            user_email = user_profile["email"]
            user_phone = user_profile["phone"]
        else:
            # Anonymous user
            geo_profile = random.choice(self.geographic_profiles)
            city = random.choice(geo_profile.cities)
            user_name = self.faker.name()
            user_email = self.faker.email()
            user_phone = self._generate_phone_number(geo_profile)
        
        # Handle first purchase logic with date consistency
        is_first_purchase = False
        if user_id:
            if user_id not in self.user_first_purchase_dates:
                # This is the user's first purchase
                is_first_purchase = True
                self.user_first_purchase_dates[user_id] = current_time
                self.seen_users.add(user_id)
            else:
                # User has purchased before - ensure date is after first purchase
                first_purchase_date = self.user_first_purchase_dates[user_id]
                if current_time <= first_purchase_date:
                    # Adjust current time to be after first purchase
                    time_diff = timedelta(hours=random.randint(1, 24*30))
                    current_time = first_purchase_date + time_diff
                is_first_purchase = False
        
        # MEMORY OPTIMIZATION: Generate product on-demand
        product_id = random.randint(1, self.product_catalog_size)
        product = self._generate_product_on_demand(geo_profile.country, product_id)
        
        # Generate quantity (with occasional negative for returns)
        quantity = random.randint(1, 5)
        if random.random() < 0.02:  # 2% chance of returns
            quantity = -quantity
        
        # Generate feedback score
        feedback_score = None
        if random.random() > 0.12:  # 88% chance of having feedback
            if random.random() < 0.08:  # 8% chance of low score
                feedback_score = random.randint(1, 3)
            else:  # 92% chance of good score
                feedback_score = random.randint(4, 5)
        
        # Marketing data
        referral_source = random.choice(self.marketing_campaigns["referral_sources"]) if random.random() > 0.18 else None
        campaign = random.choice(self.marketing_campaigns["campaigns"]) if random.random() > 0.25 else None
        coupon_code = random.choice(self.marketing_campaigns["coupon_codes"]) if random.random() > 0.35 else None
        coupon_code_flag = True if coupon_code else None
        
        # Enhanced device and browser diversity
        device_options = ["desktop", "mobile", "tablet", "other"]
        device_weights = [35, 50, 12, 3]
        device_type = random.choices(device_options, weights=device_weights)[0]
        
        browser_options = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet", "Opera", "Other"]
        browser_weights = [45, 25, 12, 8, 5, 3, 2]
        browser = random.choices(browser_options, weights=browser_weights)[0]
        
        # Handle product price - sometimes as string with currency symbols
        base_price = product["base_price"] * random.uniform(0.85, 1.25)
        if random.random() < 0.08:  # 8% as string with currency
            currency_symbols = {"USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "CAD": "C$", "AUD": "A$"}
            symbol = currency_symbols.get(geo_profile.currency, "$")
            product_price = f"{symbol}{round(base_price, 2)}"
        else:
            product_price = round(base_price, 2)
        
        # Enhanced payment methods
        payment_methods = ["credit_card", "paypal", "apple_pay", "google_pay", "stripe", "bank_transfer", "cash_on_delivery"]
        payment_weights = [40, 20, 15, 10, 8, 4, 3]
        payment_method = random.choices(payment_methods, weights=payment_weights)[0]
        
        # FIXED: Generate ALL logical combinations of transaction outcomes
        # Define all possible transaction statuses
        transaction_statuses = ["success", "failed", "cancelled", "pending", "refunded"]
        # Define all possible payment statuses
        payment_statuses = ["completed", "failed", "pending", "cancelled", "refunded"]
        # Define all possible action types
        action_types = ["purchase", "refund", "cancellation", "return"]
        
        # Generate all logical combinations with realistic weights
        transaction_outcomes = []
        
        # Most common: successful purchases
        transaction_outcomes.extend([("success", "completed", "purchase")] * 70)
        
        # Common: pending transactions
        transaction_outcomes.extend([("pending", "pending", "purchase")] * 10)
        
        # Less common but realistic: failures
        transaction_outcomes.extend([("failed", "failed", "purchase")] * 5)
        transaction_outcomes.extend([("cancelled", "cancelled", "purchase")] * 5)
        transaction_outcomes.extend([("cancelled", "failed", "purchase")] * 2)
        
        # Refunds and returns
        transaction_outcomes.extend([("success", "completed", "refund")] * 3)
        transaction_outcomes.extend([("refunded", "refunded", "refund")] * 2)
        transaction_outcomes.extend([("success", "completed", "return")] * 2)
        
        # Edge cases
        transaction_outcomes.extend([("pending", "failed", "purchase")] * 1)
        
        transaction_status, payment_status, action_type = random.choice(transaction_outcomes)
        
        # ENHANCED SHIPPING ADDRESS LOGIC: Get shipping address AFTER determining action_type
        # This ensures consistent address assignment per user while respecting purchase constraints
        if user_id:
            shipping_address = self._get_user_shipping_address(user_id, user_profile, action_type)
        else:
            # Anonymous users - enhanced logic for consistency
            if action_type == "purchase":
                # MUST have address for purchases, even anonymous users
                shipping_address = self._generate_valid_shipping_address(geo_profile, city)
            else:
                # Non-purchase transactions can have null address for anonymous users
                if random.random() > 0.08:  # 92% chance of having address
                    shipping_address = self._generate_valid_shipping_address(geo_profile, city)
                else:
                    shipping_address = None
        
        # Generate postal code consistent with country
        postal_code = None
        if random.random() > 0.06:  # 94% chance of having postal code
            if geo_profile.country == "United States":
                postal_code = f"{random.randint(10000, 99999)}"
            elif geo_profile.country == "United Kingdom":
                postal_code = f"{random.choice(['SW', 'E', 'W', 'N'])}{random.randint(1, 20)} {random.randint(1, 9)}{random.choice(['AA', 'BB'])}"
            elif geo_profile.country == "Canada":
                postal_code = f"{random.choice(['K', 'M', 'N'])}{random.randint(1, 9)}{random.choice(['A', 'B'])} {random.randint(1, 9)}{random.choice(['X', 'Y'])}{random.randint(1, 9)}"
            else:
                postal_code = f"{random.randint(10000, 99999)}"
        
        record = {
            "event_id": str(uuid.uuid4()),
            "quantity": quantity,
            "total_amount": None,
            "feedback_score": feedback_score,
            "user_id": user_id,
            "user_name": user_name,
            "user_email": user_email,
            "user_phone": user_phone,
            "is_first_purchase": is_first_purchase,
            "language": geo_profile.language,
            "currency": geo_profile.currency,
            "city_name": city,
            "country_name": geo_profile.country,
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "product_category": product["product_category"],
            "product_brand": product["product_brand"],
            "product_price": product_price,
            "shipping_address": shipping_address,
            "postal_code": postal_code,
            "device_type": device_type,
            "browser": browser,
            "referral_source": referral_source,
            "campaign": campaign,
            "coupon_code": coupon_code,
            "coupon_code_flag": coupon_code_flag,
            "payment_method": payment_method,
            "payment_status": payment_status,
            "transaction_status": transaction_status,
            "action_type": action_type,
            "event_timestamp": current_time.isoformat() + "Z" if random.random() > 0.01 else None,
            "dt_date": current_time.strftime("%Y-%m-%d"),
            "year": current_time.year,
            "month": current_time.month,
            "day": current_time.day,
            "hour": current_time.hour
        }
        
        return record
    
    def _generate_phone_number(self, geo_profile: GeographicProfile) -> Optional[str]:
        """
        Generate realistic phone number based on geographic profile.
        
        Args:
            geo_profile (GeographicProfile): Geographic profile for phone number format
            
        Returns:
            Optional[str]: Formatted phone number or None
        """
        if random.random() < 0.12:  # 12% chance of no phone
            return None
        
        if random.random() < 0.05:  # 5% chance of invalid phone
            return "invalid-phone"
        
        if geo_profile.country == "United States":
            return f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        elif geo_profile.country == "United Kingdom":
            return f"+44-{random.randint(20, 79)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
        elif geo_profile.country == "Germany":
            return f"+49-{random.randint(30, 89)}-{random.randint(10000000, 99999999)}"
        elif geo_profile.country == "France":
            return f"+33-{random.randint(1, 9)}-{random.randint(10000000, 99999999)}"
        elif geo_profile.country == "Egypt":
            return f"+20-{random.choice([10, 11, 12, 15])}-{random.randint(10000000, 99999999)}"
        elif geo_profile.country == "Saudi Arabia":
            return f"+966-5{random.randint(0, 9)}-{random.randint(1000000, 9999999)}"
        elif geo_profile.country == "UAE":
            return f"+971-5{random.randint(0, 9)}-{random.randint(1000000, 9999999)}"
        else:
            return f"{geo_profile.phone_prefix}-{random.randint(100000000, 999999999)}"
    
    def generate_batch_records(self, batch_size: int) -> Generator[Dict, None, None]:
        """
        Generate batch records using a generator to minimize memory usage.
        
        Args:
            batch_size (int): Number of records to generate
            
        Yields:
            Dict: Transaction record
        """
        # MEMORY OPTIMIZATION: Use generator instead of creating full batch in memory
        for _ in range(batch_size):
            if self.shutdown_requested:
                break
            yield self.generate_record()
    
    def _send_batch_async(self, producer: Producer, batch_size: int) -> Dict:
        """
        Send batch asynchronously with memory-efficient processing.
        
        Args:
            producer (Producer): Kafka producer instance
            batch_size (int): Size of batch to generate and send
            
        Returns:
            Dict: Batch processing results and metrics
        """
        batch_id = str(uuid.uuid4())[:8]
        batch_start_time = time.time()
        successful_sends = 0
        failed_sends = 0
        
        # MEMORY OPTIMIZATION: Minimal logging during batch processing
        try:
            # MEMORY OPTIMIZATION: Process records one by one using generator
            for i, record in enumerate(self.generate_batch_records(batch_size)):
                if self.shutdown_requested:
                    break
                    
                try:
                    # Add corruption for testing
                    if random.random() < CORRUPTION_PROBABILITY:
                        record = self._create_corrupted_record()
                    
                    # Create serialization context
                    serialization_context = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
                    
                    # Serialize key and value
                    serialized_key = self.string_serializer(record.get("user_id", f"batch_{batch_id}_{i}"))
                    serialized_value = self.avro_serializer(record, serialization_context)
                    
                    # Send to Kafka
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=serialized_key,
                        value=serialized_value
                    )
                    
                    successful_sends += 1
                    
                    # Poll for delivery reports periodically
                    if i % 1000 == 0:  # Less frequent polling
                        producer.poll(0)
                        
                        # MEMORY OPTIMIZATION: Force garbage collection periodically
                        if i % 5000 == 0:
                            gc.collect()
                        
                except Exception:
                    failed_sends += 1
            
            # Final flush to ensure all messages are sent
            producer.flush(timeout=30)
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            
            # Update counters
            self.total_sent += successful_sends
            self.total_failed += failed_sends
            self.batch_count += 1
            
            if successful_sends > 0:
                self.last_successful_send = time.time()
                self.consecutive_failures = 0
            else:
                self.consecutive_failures += 1
            
            result = {
                "batch_id": batch_id,
                "successful_sends": successful_sends,
                "failed_sends": failed_sends,
                "batch_duration": round(batch_duration, 2),
                "records_per_second": round(successful_sends / batch_duration, 2) if batch_duration > 0 else 0
            }
            
            # MEMORY OPTIMIZATION: Minimal logging
            logger.info(f"✅ Batch {batch_id}: {successful_sends:,} sent, {failed_sends} failed, {result['records_per_second']}/sec")
            sys.stdout.flush()
            return result
            
        except Exception as e:
            logger.error(f"❌ Batch {batch_id} failed: {str(e)}")
            sys.stdout.flush()
            
            self.consecutive_failures += 1
            return {
                "batch_id": batch_id,
                "successful_sends": 0,
                "failed_sends": batch_size,
                "batch_duration": time.time() - batch_start_time,
                "records_per_second": 0,
                "error": str(e)
            }
        finally:
            # MEMORY OPTIMIZATION: Force garbage collection after each batch
            gc.collect()
    
    def _create_corrupted_record(self) -> Dict:
        """
        Create a corrupted record for testing error handling.
        
        Returns:
            Dict: Corrupted test record
        """
        current_time = self._get_realistic_timestamp()
        
        corrupted_record = {
            "event_id": str(uuid.uuid4()),
            "quantity": 1,
            "total_amount": None,
            "feedback_score": None,
            "user_id": "corrupted_user",
            "user_name": "Corrupted Test",
            "user_email": "corrupted@test.com",
            "user_phone": None,
            "is_first_purchase": False,
            "language": "en",
            "currency": "USD",
            "city_name": "Test City",
            "country_name": "Test Country",
            "product_id": 999999,
            "product_name": "Corrupted Product",
            "product_category": "Test",
            "product_brand": "Test Brand",
            "product_price": 99.99,
            "shipping_address": None,
            "postal_code": None,
            "device_type": "desktop",
            "browser": "Test",
            "referral_source": None,
            "campaign": None,
            "coupon_code": None,
            "coupon_code_flag": None,
            "payment_method": "test",
            "payment_status": "corrupted",
            "transaction_status": "corrupted",
            "action_type": "corruption_test",
            "event_timestamp": current_time.isoformat() + "Z",
            "dt_date": current_time.strftime("%Y-%m-%d"),
            "year": current_time.year,
            "month": current_time.month,
            "day": current_time.day,
            "hour": current_time.hour
        }
        
        return corrupted_record
    
    def _signal_handler(self, signum, frame):
        """
        Handle graceful shutdown signals for clean application termination.
        
        Args:
            signum (int): Signal number received
            frame: Current stack frame
        """
        logger.info(f"🛑 Received signal {signum}, initiating graceful shutdown...")
        sys.stdout.flush()
        self.shutdown_requested = True
    
    def _print_performance_stats(self):
        """Print current performance statistics with minimal memory footprint."""
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        
        if elapsed_time > 0:
            overall_rate = self.total_sent / elapsed_time
            
            # MEMORY OPTIMIZATION: Simplified logging
            logger.info(f"📊 Stats: Sent={self.total_sent:,}, Failed={self.total_failed:,}, Rate={round(overall_rate, 2)}/sec, Users={len(self.seen_users):,}")
            sys.stdout.flush()
    
    def run(self):
        """Main execution loop with memory-optimized processing."""
        logger.info("🎬 Starting Memory-Optimized ECS Data Generation")
        logger.info(f"📋 Config: Duration={SCRIPT_DURATION_SECONDS/60}min, Batch={RECORDS_PER_BATCH:,}, Interval={BATCH_INTERVAL_SECONDS}sec")
        logger.info("🔧 FIXED: Purchase transactions guaranteed to have valid shipping addresses")
        logger.info("🔧 ENHANCED: Users limited to 1-2 consistent shipping addresses")
        sys.stdout.flush()
        
        last_health_check = time.time()
        
        try:
            while not self.shutdown_requested and time.time() - self.start_time < SCRIPT_DURATION_SECONDS:
                batch_start_time = time.time()
                
                try:
                    # Create/recreate producer if needed
                    producer = self._create_kafka_producer()
                    
                    # MEMORY OPTIMIZATION: Process batch using generator
                    batch_result = self._send_batch_async(producer, RECORDS_PER_BATCH)
                    
                    # Health check and stats
                    current_time = time.time()
                    if current_time - last_health_check >= HEALTH_CHECK_INTERVAL:
                        self._print_performance_stats()
                        last_health_check = current_time
                    
                    # Wait for next batch interval
                    batch_duration = time.time() - batch_start_time
                    sleep_time = max(0, BATCH_INTERVAL_SECONDS - batch_duration)
                    
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                
                except KeyboardInterrupt:
                    logger.info("⌨️ Keyboard interrupt received")
                    sys.stdout.flush()
                    self.shutdown_requested = True
                    break
                    
                except Exception as e:
                    logger.error(f"❌ Error in main loop: {str(e)}")
                    sys.stdout.flush()
                    
                    # Exponential backoff on errors
                    self.consecutive_failures += 1
                    backoff = min(60, RETRY_BACKOFF_BASE ** min(self.consecutive_failures, 6))
                    time.sleep(backoff)
        
        finally:
            # Cleanup
            if self.producer:
                try:
                    logger.info("🧹 Flushing Kafka producer...")
                    sys.stdout.flush()
                    self.producer.flush(timeout=10)
                    logger.info("✅ Producer cleanup completed")
                    sys.stdout.flush()
                except Exception as cleanup_error:
                    logger.error(f"❌ Error during cleanup: {str(cleanup_error)}")
                    sys.stdout.flush()
            
            # Final stats
            self._print_performance_stats()
            
            total_time = time.time() - self.start_time
            logger.info("🏁 Memory-Optimized Data Generation Completed")
            logger.info(f"🏁 Final Stats: Runtime={round(total_time / 60, 2)}min, Sent={self.total_sent:,}, Failed={self.total_failed:,}")
            logger.info(f"🏁 Final Rate: {round(self.total_sent / total_time, 2)}/sec" if total_time > 0 else "🏁 Final Rate: 0/sec")
            logger.info(f"🏁 Users Generated: {len(self.seen_users):,}, Countries: {len(self.geographic_profiles)}")
            logger.info("✅ Shipping address constraint: All purchase transactions have valid addresses")
            logger.info("✅ Address consistency: Users limited to 1-2 reusable shipping addresses")
            sys.stdout.flush()


def main():
    """Main entry point with memory-optimized error handling."""
    try:
        logger.info("🚀 Starting Memory-Optimized ECS E-commerce Data Generator")
        logger.info(f"🔧 Python: {sys.version}")
        logger.info(f"🔧 Log Level: {os.getenv('LOG_LEVEL', 'INFO')}")
        logger.info("🔧 CONSTRAINT: Purchase transactions require valid shipping addresses")
        logger.info("🔧 ENHANCEMENT: Users limited to 1-2 consistent shipping addresses")
        sys.stdout.flush()
        
        # Initialize and run the memory-optimized generator
        generator = ModernAvroDataGenerator()
        generator.run()
        
    except KeyboardInterrupt:
        logger.info("⌨️ Received keyboard interrupt, shutting down...")
        sys.stdout.flush()
        
    except Exception as e:
        logger.error(f"💥 Fatal error: {str(e)}")
        logger.error(f"💥 Error type: {type(e).__name__}")
        logger.error(f"💥 Traceback: {traceback.format_exc()}")
        sys.stdout.flush()
        sys.exit(1)
    
    finally:
        logger.info("👋 Memory-Optimized Data Generator Shutdown Complete")
        sys.stdout.flush()


if __name__ == "__main__":
    main()