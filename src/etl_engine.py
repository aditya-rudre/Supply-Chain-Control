import pandas as pd
import sqlite3
import logging
from sqlalchemy import create_engine
import os

# Setup Logging
logging.basicConfig(
    filename='pipeline.log', 
    level=logging.INFO, 
    format='%(asctime)s:%(levelname)s:%(message)s'
)

class SupplyChainETL:
    """
    ETL Pipeline class to extract Supply Chain data, transform into
    Star Schema, and load into SQLite Data Warehouse.
    """
    def __init__(self, db_path, data_path, sql_schema_path):
        self.db_engine = create_engine(f'sqlite:///{db_path}')
        self.data_path = data_path
        self.sql_schema_path = sql_schema_path
        self.df = None
        
    def extract(self):
        """Loads raw data handling encoding issues and cleans column names."""
        try:
            logging.info("Starting Extraction...")
            self.df = pd.read_csv(self.data_path, encoding='ISO-8859-1')
            
            # Clean column names (strip spaces, lowercase, replace spaces with underscores)
            self.df.columns = [
                c.strip().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').lower() 
                for c in self.df.columns
            ]
            logging.info(f"Extraction Success. Loaded {len(self.df)} rows.")
        except Exception as e:
            logging.error(f"Extraction Failed: {e}")
            raise e

    def transform_dimensions(self):
        """Splits raw data into Dimensions for Star Schema."""
        logging.info("Transforming Dimensions...")
        
        # 1. Dim Customers
        self.dim_customers = self.df[[
            'customer_id', 'customer_fname', 'customer_lname', 
            'customer_segment', 'customer_city', 'customer_state', 'customer_country'
        ]].drop_duplicates(subset=['customer_id']).rename(columns={
            'customer_fname': 'f_name', 'customer_lname': 'l_name',
            'customer_segment': 'segment', 'customer_city': 'city',
            'customer_state': 'state', 'customer_country': 'country'
        })
        
        # 2. Dim Products
        self.dim_products = self.df[[
            'product_card_id', 'product_name', 'category_name', 
            'department_name', 'product_price'
        ]].drop_duplicates(subset=['product_card_id'])
        
        # 3. Dim Location (Surrogate Key)
        self.dim_location = self.df[[
            'market', 'order_region', 'order_country', 'order_city'
        ]].drop_duplicates().reset_index(drop=True)
        self.dim_location['location_id'] = self.dim_location.index + 1

    def transform_fact(self):
        """Creates the Fact Table mapping foreign keys."""
        logging.info("Transforming Fact Table...")
        
        # Merge to get location_id back into the main dataframe
        df_merged = self.df.merge(
            self.dim_location, 
            on=['market', 'order_region', 'order_country', 'order_city'], 
            how='left'
        )

        # Fix Dates
        df_merged['order_date'] = pd.to_datetime(df_merged['order_date_dateorders'])
        df_merged['shipping_date'] = pd.to_datetime(df_merged['shipping_date_dateorders'])

        # Select Fact Columns
        self.fact_orders = df_merged[[
            'order_id', 'order_item_id', 'customer_id', 'product_card_id', 'location_id',
            'order_date', 'shipping_date', 'shipping_mode',
            'days_for_shipment_scheduled', 'days_for_shipping_real', 
            'delivery_status', 'order_status',  # Added order_status here
            'benefit_per_order', 'sales', 'order_item_quantity', 
            'late_delivery_risk'
        ]].rename(columns={
            'days_for_shipment_scheduled': 'days_scheduled',
            'days_for_shipping_real': 'days_real',
            'sales': 'sales_amount',
            'order_item_quantity': 'order_quantity'
        })

    def load(self):
        """Loads data into SQLite using SQLAlchemy."""
        logging.info("Loading to Database...")
        try:
            # Execute schema creation
            with open(self.sql_schema_path, 'r') as f:
                schema_sql = f.read()
            
            with self.db_engine.connect() as conn:
                conn.connection.executescript(schema_sql)

            # Load Dataframes
            self.dim_customers.to_sql('dim_customers', self.db_engine, if_exists='append', index=False)
            self.dim_products.to_sql('dim_products', self.db_engine, if_exists='append', index=False)
            self.dim_location.to_sql('dim_location', self.db_engine, if_exists='append', index=False)
            self.fact_orders.to_sql('fact_orders', self.db_engine, if_exists='append', index=False)
            
            logging.info("Load Complete. Data Warehouse is ready.")
            print("Pipeline executed successfully.")
        except Exception as e:
            logging.error(f"Load Failed: {e}")
            raise e

    def run(self):
        self.extract()
        self.transform_dimensions()
        self.transform_fact()
        self.load()