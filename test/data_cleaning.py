import polars as pl
import random

def clean_duplicate_columns(df):
    """
    Drops empty columns and combines columns with the same name (ignoring spaces and case),
    taking the first non-null value, then restores original formatting.
    """
    # Drop empty columns first
    empty_cols = [col for col in df.columns if df[col].null_count() == df.height]
    if empty_cols:
        print(f"Dropping {len(empty_cols)} empty columns")
        df = df.drop(empty_cols)
    
    # Create mapping of normalized names to original names
    normalized_to_original = {}
    for col in df.columns:
        normalized = col.replace(' ', '').lower()
        if normalized not in normalized_to_original:
            normalized_to_original[normalized] = []
        normalized_to_original[normalized].append(col)
    
    # Find duplicates and combine them
    combined_df = df
    final_column_names = []
    
    for normalized, original_cols in normalized_to_original.items():
        if len(original_cols) > 1:
            # Multiple columns with same normalized name - combine them
            print(f"Combining columns: {original_cols}")
            
            # Create combined column using coalesce (first non-null value)
            combined_col = combined_df[original_cols[0]]
            for col in original_cols[1:]:
                combined_col = combined_col.fill_null(combined_df[col])
            
            # Pick random original name for the final column
            chosen_name = random.choice(original_cols)
            final_column_names.append(chosen_name)
            
            # Drop original columns and add combined one
            combined_df = combined_df.drop(original_cols)
            combined_df = combined_df.with_columns(combined_col.alias(chosen_name))
            
        else:
            # Single column - keep as is
            final_column_names.append(original_cols[0])
    
    # Reorder columns to match final_column_names
    combined_df = combined_df.select(final_column_names)
    
    # Convert 0/1 columns to boolean
    for col in combined_df.columns:
        unique_values = combined_df[col].drop_nulls().unique().to_list()
        
        # Check if column should be converted to boolean
        numeric_values = []
        non_numeric_values = []
        
        for val in unique_values:
            try:
                num_val = float(val)
                numeric_values.append(num_val)
            except (ValueError, TypeError):
                non_numeric_values.append(val)
        
        # Convert if: only 0 and 1, or 0/1 with one non-numeric value
        if len(numeric_values) <= 2 and set(numeric_values).issubset({0.0, 1.0}):
            if len(numeric_values) == 2 or (len(numeric_values) == 1 and len(non_numeric_values) <= 1):
                print(f"Converting column '{col}' to boolean")
                combined_df = combined_df.with_columns(
                    pl.when(combined_df[col] == "1").then(True)
                    .when(combined_df[col] == 1).then(True)
                    .when(combined_df[col] == "0").then(False)
                    .when(combined_df[col] == 0).then(False)
                    .otherwise(None)
                    .alias(col)
                )
    
    return combined_df

# Test the function
if __name__ == "__main__":
    # Load the survey data
    df = pl.read_csv("survey.csv", encoding="latin-1", infer_schema_length=10000, ignore_errors=True)
    
    print(f"Original columns: {len(df.columns)}")
    
    # Clean duplicate columns (includes dropping empty columns)
    cleaned_df = clean_duplicate_columns(df)
    
    print(f"After cleaning: {len(cleaned_df.columns)} columns")
    print(f"Removed {len(df.columns) - len(cleaned_df.columns)} columns total")