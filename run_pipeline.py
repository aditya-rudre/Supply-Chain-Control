from src.etl_engine import SupplyChainETL

def main():
    # Define Paths
    DB_PATH = 'database/supply_chain_dw.db'
    DATA_PATH = 'data/DataCoSupplyChainDataset.csv'
    SCHEMA_PATH = 'src/schema_queries.sql'

    # Initialize and Run Pipeline
    etl = SupplyChainETL(DB_PATH, DATA_PATH, SCHEMA_PATH)
    etl.run()

if __name__ == "__main__":
    main() 