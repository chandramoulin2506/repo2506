import os
import re
import csv

# Define directories relative to the current working directory
cwd = os.getcwd()
sql_folder = os.path.join(cwd, 'utils', 'sql')
output_sql_folder = os.path.join(cwd, 'utils', 'sql_out')
csv_file_path = os.path.join(output_sql_folder, 'temp_table_mapping.csv')
replacement_mapping_file = os.path.join(output_sql_folder, 'replacement_mapping.csv')

def load_csv_mappings(csv_file_path):
    """Load mappings from the CSV file."""
    mappings = {}
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            temp_table = row['temp_table_name']
            replaced_table = row['replaced_table_name']
            mappings[replaced_table] = temp_table
    return mappings

def process_sql_file(file_path, table_mappings):
    with open(file_path, 'r') as file:
        content = file.readlines()  # Read file as lines for line tracking

    updated_content = []
    replacements = []  # To store line number and replacement info

    # Iterate through lines to perform replacements
    for line_number, line in enumerate(content, start=1):
        original_line = line  # Store original line for reference

        # Check for CREATE OR REPLACE TABLE
        for replaced_table, temp_table in table_mappings.items():
            # Replace CREATE OR REPLACE TABLE with CREATE TEMPORARY TABLE
            create_pattern = f'CREATE OR REPLACE TABLE {replaced_table}'
            if create_pattern in line:
                line = line.replace(create_pattern, f'CREATE TEMPORARY TABLE {temp_table}')
                replacements.append((line_number, original_line.strip(), f'CREATE TEMPORARY TABLE {temp_table}', 'CREATE'))

            # Replace occurrences in joins and selects
            if replaced_table in line:
                line = line.replace(replaced_table, temp_table)
                replacements.append((line_number, original_line.strip(), temp_table, 'JOIN'))

        updated_content.append(line)

    return updated_content, replacements

def write_output_files():
    # Load the mappings from the CSV
    table_mappings = load_csv_mappings(csv_file_path)

    all_replacements = []  # To store all replacements across files

    # Write updated SQL files
    for filename in os.listdir(sql_folder):
        if filename.endswith('.sql'):
            input_path = os.path.join(sql_folder, filename)
            output_path = os.path.join(output_sql_folder, filename)
            updated_content, replacements = process_sql_file(input_path, table_mappings)
            with open(output_path, 'w') as file:
                file.writelines(updated_content)

            # Collect replacements for this file
            if replacements:
                all_replacements.extend([(filename, *replacement) for replacement in replacements])

    # Write the replacement mapping to a CSV file
    with open(replacement_mapping_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['file_name', 'line_number', 'original_line', 'replaced_line', 'replacement_type'])
        writer.writerows(all_replacements)

def main():
    write_output_files()
    print("Processing complete. Check the output folder for reversed SQL files and replacement mapping.")

if __name__ == '__main__':
    main()
