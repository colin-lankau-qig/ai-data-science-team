# Test Scripts

This folder contains two Python scripts for processing and analyzing survey data.

## Files

### `data_cleaning.py`
A data cleaning utility that processes survey datasets by:
- **Dropping empty columns** that contain only null values
- **Combining duplicate columns** with similar names (ignoring spaces and case differences)
- **Converting 0/1 columns to boolean** for better data type consistency
- **Preserving original column formatting** while handling data inconsistencies

**Key function**: `clean_duplicate_columns(df)` - Takes a Polars DataFrame and returns a cleaned version

### `convert_survey.py`
A survey analysis script that:
- **Loads survey data** from `survey.csv` using latin-1 encoding
- **Applies data cleaning** using the `clean_duplicate_columns` function
- **Generates value counts** for each column to analyze response distributions
- **Exports analysis results** to `survey_analysis_cleaned.json` in a structured format

## Usage

```bash
# Run data cleaning test
python data_cleaning.py

# Run survey conversion and analysis
python convert_survey.py
```

## Dependencies
- `polars` - Fast DataFrame library for data manipulation
- `json` - Standard library for JSON operations
- `random` - Used for column name selection during duplicate handling

## Output Files
- `survey_analysis_cleaned.json` - Contains value counts and analysis for each survey column