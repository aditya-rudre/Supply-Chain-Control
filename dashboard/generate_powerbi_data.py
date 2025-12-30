import pandas as pd
import sqlite3
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
DB_PATH = 'database/supply_chain_dw.db'
OUTPUT_DIR = 'powerbi_data'

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def load_data_from_db():
    """Loads necessary tables from the SQLite Data Warehouse."""
    print("Loading data from database...")
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found at {DB_PATH}. Please run your ETL pipeline first.")

    conn = sqlite3.connect(DB_PATH)

    # Load Fact Table joined with necessary Dimension keys
    # We join with dims temporarily to get features for the ML model (market, region, category)
    fact_query = """
    SELECT 
        f.*,
        d.market,
        d.order_region,
        p.category_name
    FROM fact_orders f
    LEFT JOIN dim_location d ON f.location_id = d.location_id
    LEFT JOIN dim_products p ON f.product_card_id = p.product_card_id
    """
    df_fact = pd.read_sql(fact_query, conn)

    # Load Dimension Tables separately for export (Pure Star Schema)
    df_dim_customers = pd.read_sql("SELECT * FROM dim_customers", conn)
    df_dim_products = pd.read_sql("SELECT * FROM dim_products", conn)
    df_dim_location = pd.read_sql("SELECT * FROM dim_location", conn)

    conn.close()
    return df_fact, df_dim_customers, df_dim_products, df_dim_location

def train_and_predict(df):
    """
    Trains a Random Forest model on the historical data and 
    appends predictions to the dataframe.
    """
    print("Training model and generating predictions...")
    
    # 1. Prepare Features (Encode categorical variables)
    le_shipping = LabelEncoder()
    le_market = LabelEncoder()
    le_category = LabelEncoder()

    # Fill missing values to ensure model stability
    df['shipping_mode'] = df['shipping_mode'].fillna('Unknown')
    df['market'] = df['market'].fillna('Unknown')
    df['category_name'] = df['category_name'].fillna('Unknown')
    df['days_scheduled'] = df['days_scheduled'].fillna(df['days_scheduled'].mean())

    # Create encoded columns
    df['shipping_mode_code'] = le_shipping.fit_transform(df['shipping_mode'])
    df['market_code'] = le_market.fit_transform(df['market'])
    df['category_code'] = le_category.fit_transform(df['category_name'])
    
    # Define features and target (Same as your notebook)
    feature_cols = ['shipping_mode_code', 'days_scheduled', 'market_code', 'category_code']
    X = df[feature_cols]
    y = df['late_delivery_risk'].fillna(0) # Target

    # 2. Train Model (Random Forest)
    # Using n_estimators=50 for speed in generation
    rf_model = RandomForestClassifier(n_estimators=50, random_state=42, class_weight='balanced')
    rf_model.fit(X, y)

    # 3. Generate Predictions for the ENTIRE dataset
    # Predicted Class: 0 (On Time) or 1 (Late)
    df['Predicted_Late_Delivery'] = rf_model.predict(X)
    
    # Prediction Confidence: Probability (0.0 to 1.0) of being Late
    df['Prediction_Confidence'] = rf_model.predict_proba(X)[:, 1]

    print("Predictions generated successfully.")
    
    return df

def export_to_csv(df_fact, df_cust, df_prod, df_loc):
    """Exports cleaned tables to CSV for Power BI."""
    print(f"Exporting files to '{OUTPUT_DIR}'...")

    # 1. Export Fact Table
    # Drop the temporary text columns used for ML training (they exist in Dims)
    # But keep the new Prediction columns
    cols_to_drop = ['shipping_mode_code', 'market_code', 'category_code', 'market', 'order_region', 'category_name']
    
    final_fact = df_fact.drop(columns=cols_to_drop, errors='ignore')
    
    final_fact.to_csv(f"{OUTPUT_DIR}/Fact_Shipments.csv", index=False)
    print(" - Fact_Shipments.csv saved.")

    # 2. Export Dimensions
    df_cust.to_csv(f"{OUTPUT_DIR}/Dim_Customers.csv", index=False)
    print(" - Dim_Customers.csv saved.")

    df_prod.to_csv(f"{OUTPUT_DIR}/Dim_Products.csv", index=False)
    print(" - Dim_Products.csv saved.")

    df_loc.to_csv(f"{OUTPUT_DIR}/Dim_Location.csv", index=False)
    print(" - Dim_Location.csv saved.")

def generate_scenarios():
    """Generates a hypothetical 'What-If' parameter table for Power BI."""
    print("Generating What-If Scenarios...")
    
    # This table allows the user to slice by "What if we changed shipping mode?"
    data = {
        'Scenario_Mode': ['Standard Class', 'Second Class', 'First Class', 'Same Day'],
        'Cost_Factor': [1.0, 1.2, 1.5, 2.0], # Hypothetical cost increase
        'Speed_Factor': [1.0, 1.1, 1.3, 1.5] # Hypothetical speed boost
    }
    df_scenarios = pd.DataFrame(data)
    df_scenarios.to_csv(f"{OUTPUT_DIR}/Param_Scenarios.csv", index=False)
    print(" - Param_Scenarios.csv saved.")

def main():
    try:
        # 1. Load Data
        df_fact, df_cust, df_prod, df_loc = load_data_from_db()
        
        # 2. Train Model & Enrich Fact Table
        df_fact_enriched = train_and_predict(df_fact)
        
        # 3. Export Tables
        export_to_csv(df_fact_enriched, df_cust, df_prod, df_loc)
        
        # 4. Generate Scenarios
        generate_scenarios()
        
        print("\nSUCCESS! Data is ready for Power BI.")
        print(f"Files are located in: {os.path.abspath(OUTPUT_DIR)}")
        
    except Exception as e:
        print(f"\nERROR: {e}")

if __name__ == "__main__":
    main()