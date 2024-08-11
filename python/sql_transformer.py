import os
import re
import csv

# Define directories relative to the current working directory
cwd = os.getcwd()
sql_folder = os.path.join(cwd, 'utils', 'input')
output_sql_folder = os.path.join(cwd, 'utils', 'output')
drop_file_path = os.path.join(output_sql_folder, 'drop.sql')
csv_file_path = os.path.join(output_sql_folder, 'temp_table_mapping.csv')

project_id = 'my-project'
dataset_id = 'my_dataset'

# Regular expression patterns for temporary table names
# Pattern to find CREATE TEMPORARY TABLE statements
temp_table_create_pattern = re.compile(r'CREATE TEMPORARY TABLE (\w+_\{\{get_macro\(dag_run\)\["\w+"\]\}\})', re.IGNORECASE)
# Pattern to find any usage of temporary table names in the SQL
temp_table_use_pattern = re.compile(r'\b(\w+_\{\{get_macro\(dag_run\)\["\w+"\]\}\})\b', re.IGNORECASE)

# Data structures for replacements
temp_table_mappings = {}
drop_statements_by_file = {}

def process_sql_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        
    # Find temporary tables to replace
    temp_tables = temp_table_create_pattern.findall(content)
    
    # Replace CREATE TEMPORARY TABLE with CREATE OR REPLACE TABLE
    for temp_table in temp_tables:
        replaced_table = f'{project_id}.{dataset_id}.{temp_table}'
        temp_table_mappings[temp_table] = replaced_table
        
        # Replace the creation statement
        content = content.replace(f'CREATE TEMPORARY TABLE {temp_table}', f'CREATE OR REPLACE TABLE {replaced_table}')
        
        # Generate drop statements
        if file_path not in drop_statements_by_file:
            drop_statements_by_file[file_path] = []
        drop_statements_by_file[file_path].append(f'DROP TABLE IF EXISTS `{replaced_table}`;')

    # Replace all occurrences of temporary tables in the rest of the SQL
    for temp_table, replaced_table in temp_table_mappings.items():
        content = re.sub(re.escape(temp_table), replaced_table, content)

    return content

def write_output_files():
    # Write updated SQL files
    for filename in os.listdir(sql_folder):
        if filename.endswith('.sql'):
            input_path = os.path.join(sql_folder, filename)
            output_path = os.path.join(output_sql_folder, filename)
            updated_content = process_sql_file(input_path)
            with open(output_path, 'w') as file:
                file.write(updated_content)
    
    # Write drop statements with comments
    with open(drop_file_path, 'w') as file:
        for file_path, statements in drop_statements_by_file.items():
            file.write(f'---- from file {os.path.basename(file_path)}\n')
            file.write('\n'.join(statements))
            file.write('\n')

    # Write temp table mappings to CSV
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['temp_table_name', 'replaced_table_name', 'file_name'])
        for temp_table, replaced_table in temp_table_mappings.items():
            file_name = next(
                (os.path.basename(f) for f in os.listdir(sql_folder) if temp_table in open(os.path.join(sql_folder, f)).read()),
                'unknown_file.sql'
            )
            writer.writerow([temp_table, replaced_table, file_name])

def main():
    write_output_files()
    print("Processing complete. Check the output folder for updated SQL files, drop statements, and CSV mapping.")

if __name__ == '__main__':
    main()
