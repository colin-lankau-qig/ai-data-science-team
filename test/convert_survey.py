import polars as pl
import json
from data_cleaning import clean_duplicate_columns

df = pl.read_csv("survey.csv", encoding="latin-1", infer_schema_length=10000, ignore_errors=True)

# Clean the data
cleaned_df = clean_duplicate_columns(df)

# Print cleaned column names
print('\n'.join(cleaned_df.columns))

# Create analysis dictionary
analysis = {}

# Analyze each column
for column in cleaned_df.columns:
    value_counts = cleaned_df[column].value_counts()
    
    # Convert to dictionary format
    column_analysis = {}
    for row in value_counts.iter_rows():
        answer = str(row[0]) if row[0] is not None else "null"
        count = row[1]
        column_analysis[answer] = count
    
    analysis[column] = {"answer": column_analysis}

# Export to JSON file
with open("survey_analysis_cleaned.json", "w", encoding="utf-8") as f:
    json.dump(analysis, f, indent=2, ensure_ascii=False)

print(f"\nAnalysis exported to survey_analysis_cleaned.json")
print(f"Analyzed {len(analysis)} columns")

