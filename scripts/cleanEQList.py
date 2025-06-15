import pandas as pd

# === Configurations ===
file_path = "data/stockList/stockList_raw.csv"  # Updated filename

def main():
    # Step 1: Load raw CSV
    try:
        df = pd.read_csv(file_path)
        print("📄 Loaded raw stock list successfully.")
    except FileNotFoundError:
        print(f"❌ File not found at: {file_path}")
        return

    # Step 2: Filter symbol_ticker with pattern 'NSE : {stockName} - EQ'
    pattern = r'^NSE\s*:\s*.+\s*-\s*EQ$'
    filtered_df = df[df['symbol_ticker'].str.contains(pattern, regex=True, na=False)]
    print(f"✅ Total rows matching 'NSE : {{stockName}} - EQ': {len(filtered_df)}")

    # Step 3: Keep only the 'underlying_symbol' column
    filtered_df = filtered_df[['underlying_symbol']].drop_duplicates().reset_index(drop=True)

    # Step 4: Save cleaned symbols to file
    output_path = "data/stockList/stockList_clean.csv"
    filtered_df.to_csv(output_path, index=False)
    print(f"💾 Cleaned symbol list saved to: {output_path}")

if __name__ == "__main__":
    main()
