import csv
import json

INPUT_FILE = "data/input/transactions_mar.csv"
OUTPUT_DIR = "data/input/transactions_mar.json"

# Open the CSV file and read it into a list of dictionaries
with open(INPUT_FILE, mode='r', newline='', encoding='utf-8') as csvfile:
    # DictReader automatically treats the first row as headers/keys
    data = list(csv.DictReader(csvfile))

# Write the data out to a formatted JSON file
with open(OUTPUT_DIR, mode='w', encoding='utf-8') as jsonfile:
    json.dump(data, jsonfile, indent=4)