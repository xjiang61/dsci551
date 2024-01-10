import json
import csv
import os

class NoSQLDatabase:
    def __init__(self, data_dir):
        self.data_dir = os.path.abspath(data_dir)
        self.max_records_per_chunk = 1000
        self.tables = {}
        self.initialize_tables()

    def initialize_tables(self):
        """
        Initialize the self.tables dictionary with existing tables and their chunks.
        """
        for item in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, item)
            if os.path.isdir(path):
                table_name = item
                first_file = next((f for f in os.listdir(path) if f.endswith('.json')), None)
                if first_file:
                    first_file_path = os.path.join(path, first_file)
                    with open(first_file_path, "r", encoding='utf-8') as file:
                        first_record = json.load(file)[0]
                        columns = list(first_record.keys())
                        self.tables[table_name] = {
                            "columns": columns,
                            "data_dir": path
                        }

    def create_table(self, table_name: str, columns: list, overwrite_existing=False):
        if table_name.lower() in self.tables and not overwrite_existing:
            print(f"Table '{table_name}' already exists.")
            return
        
        data_dir_path = os.path.join(self.data_dir, f"{table_name.lower()}")
        os.makedirs(data_dir_path, exist_ok=True)
        data_file_path = os.path.join(data_dir_path, "chunk_0.json")
        self.tables[table_name.lower()] = {"columns": columns, "data_dir": data_dir_path}
        current = {column:"" for column in columns}

        with open(data_file_path, "w") as file:
            json.dump([current], file)

        print("Table created.")

    def insert_into(self, table_name: str, data: dict):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return

        table_info = self.tables[table_name]
        data_dir = table_info["data_dir"]
        columns = table_info["columns"]
        max_records_per_chunk = self.max_records_per_chunk

        if not all(key in columns for key in data.keys()):
            print("Data format does not match table columns.")
            return

        files = sorted([f for f in os.listdir(data_dir) if f.endswith('.json')])
        last_file = files[-1] if files else None

        if last_file:
            last_file_path = os.path.join(data_dir, last_file)
            with open(last_file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)
            
            if len(chunk_data)==1:
                chunk_data = [dic for dic in chunk_data if dic[columns[0]]]

            if len(chunk_data) < max_records_per_chunk:
                chunk_data.append(data)
            else:
                chunk_data = [data]
                last_file = f"chunk_{len(files)}.json"
                last_file_path = os.path.join(data_dir, last_file)

            with open(last_file_path, 'w', encoding='utf-8') as file:
                json.dump(chunk_data, file, indent=4)

        else:
            first_file = "chunk_0.json"
            first_file_path = os.path.join(data_dir, first_file)
            with open(first_file_path, 'w', encoding='utf-8') as file:
                json.dump([data], file, indent=4)

        print(f"Data inserted into table '{table_name}'.")
        

    def select_from(self, table_name: str, conditions: dict = None, projection: list = None,
                    group_by: str = None, aggregate: str = None, aggregate_column: str = None, order_by: str = None):
        lowercase_table_name = table_name.lower()

        if lowercase_table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return

        table_info = self.tables[lowercase_table_name]
        data_dir = table_info["data_dir"]
        aggregated_data = []

        for file_name in sorted(os.listdir(data_dir)):
            if file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)

                    # Apply conditions and projection
                    if conditions:
                        chunk_data = [record for record in chunk_data if all(record.get(col) == conditions[col] for col in conditions)]
                    if projection:
                        chunk_data = [{col: record[col] for col in projection} for record in chunk_data]

                    # Merge chunk data into aggregated data
                    aggregated_data.extend(chunk_data)
                
                
        # Apply grouping
        if group_by:
            grouped_data = {}
            for record in aggregated_data:
                group_key = record[group_by]
                if group_key not in grouped_data:
                    grouped_data[group_key] = []
                grouped_data[group_key].append(record)

        # Apply aggregation
        if aggregate:
            aggregated_result = []
            for group, records in grouped_data.items():
                if aggregate.lower() == 'count':
                    aggregated_result.append({group_by: group, 'count': len(records)})
                elif aggregate.lower() == 'sum':
                    # Sum the specified column
                    sum_values = sum(float(record.get(aggregate_column, 0)) for record in records)
                    aggregated_result.append({group_by: group, 'sum': sum_values})
                elif aggregate.lower() == 'avg':
                    # Sum the specified column
                    sum_values = sum(float(record.get(aggregate_column, 0)) for record in records)
                    aggregated_result.append({group_by: group, 'avg': sum_values / len(records)})
            aggregated_data = aggregated_result

        # Apply ordering
        if order_by:
            reverse = order_by.startswith('-')
            sort_key = order_by[1:] if reverse else order_by
            aggregated_data = self.bubble_sort(aggregated_data, sort_key, reverse=reverse)
        
        return aggregated_data
    
    def bubble_sort(self, data, key, reverse=False):
        n = len(data)
        for i in range(n):
            for j in range(0, n-i-1):
                if reverse:
                    if data[j][key] < data[j + 1][key]:
                        data[j], data[j + 1] = data[j + 1], data[j]
                else:
                    if data[j][key] > data[j + 1][key]:
                        data[j], data[j + 1] = data[j + 1], data[j]
        return data
    
    def perform_join(self, left_table_name, right_table_name, left_join_key, right_join_key, join_type='inner'):
        left_table_info = self.tables[left_table_name.lower()]
        right_table_info = self.tables[right_table_name.lower()]

        left_data_dir = left_table_info["data_dir"]
        right_data_dir = right_table_info["data_dir"]

        joined_data = []

        # Read data from the right table and index it using right_join_key
        right_records = {}
        for file_name in sorted(os.listdir(right_data_dir)):
            if file_name.endswith('.json'):
                file_path = os.path.join(right_data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    for record in json.load(file):
                        key = record.get(right_join_key)
                        if key:
                            if key not in right_records:
                                right_records[key] = []
                            right_records[key].append(record)

        # For full join, keep track of right table keys that have been matched
        matched_right_keys = set()

        # Process data from the left table and perform the join
        for file_name in sorted(os.listdir(left_data_dir)):
            if file_name.endswith('.json'):
                file_path = os.path.join(left_data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    for left_record in json.load(file):
                        left_key = left_record.get(left_join_key)
                        right_matched_records = right_records.get(left_key, [])

                        # Mark the matched records from the right table
                        if left_key in right_records:
                            matched_right_keys.add(left_key)

                        if right_matched_records and (join_type == 'inner' or join_type == 'right'):
                            for right_record in right_matched_records:
                                joined_data.append({**left_record, **right_record})
                        elif join_type in ['left', 'full']:
                            if right_matched_records:
                                for right_record in right_matched_records:
                                    joined_data.append({**left_record, **right_record})
                            else:
                                default_right_record = {key: '' for key in right_table_info['columns']}
                                joined_data.append({**left_record, **default_right_record})

        # # Process unmatched records from the right table for right and full joins
        if join_type =='full' or join_type =='right':
            for right_key, records in right_records.items():
                if right_key not in matched_right_keys:
                    for record in records:
                        default_left_record = {key: '' for key in left_table_info['columns']}
                        joined_data.append({**default_left_record, **record})

        return joined_data

    def delete_from(self, table_name: str, conditions: dict):
        lowercase_table_name = table_name.lower()

        if lowercase_table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return

        table_info = self.tables[lowercase_table_name]
        data_dir = table_info["data_dir"]
        updated_data = []

        for file_name in sorted(os.listdir(data_dir)):
            if file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)

                    # Delete data based on conditions
                    chunk_data = [record for record in chunk_data if not all(record.get(col) == conditions[col] for col in conditions)]

                    updated_data.extend(chunk_data)

                # Write back the updated data to the file
                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump(updated_data, file, indent=4)

        print(f"Data deleted from table '{table_name}'.")

    def update_table(self, table_name: str, data: dict, conditions: dict):
        lowercase_table_name = table_name.lower()

        if lowercase_table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return

        table_info = self.tables[lowercase_table_name]
        data_dir = table_info["data_dir"]
        updated_data = []

        for file_name in sorted(os.listdir(data_dir)):
            if file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)

                    # Update data based on conditions
                    for record in chunk_data:
                        if all(record.get(col) == conditions[col] for col in conditions):
                            record.update(data)

                    updated_data.extend(chunk_data)

                # Write back the updated data to the file
                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump(updated_data, file, indent=4)

        print(f"Data updated in table '{table_name}'.")


def print_table(data):
    if not data:
        print("No data to display.")
        return

    all_columns = set()
    for row in data:
        all_columns.update(row.keys())
    all_columns = list(all_columns)

    col_width = {}
    for col in all_columns:
        max_width = max(len(col), max(len(str(row.get(col, ''))) for row in data))
        col_width[col] = max_width

    header = ' | '.join(f"{col.ljust(col_width[col])}" for col in all_columns)
    print(header)
    print('-' * len(header))

    for row in data:
        row_data = [str(row.get(col, '')).ljust(col_width[col]) for col in all_columns]
        print(' | '.join(row_data))

# Usage
data_directory = "./nosql_data"
db = NoSQLDatabase(data_directory)

# CLI
while True:
    user_input = input("MyDB > ")
    tokens = user_input.split()

    if tokens[0].lower() == 'create' and tokens[1].lower() == 'table':
        table_name = tokens[2]
        columns_str = user_input.split('(')[1].split(')')[0]
        columns = [col.strip() for col in columns_str.split(',')]
        db.create_table(table_name, columns)

    elif tokens[0].lower() == 'insert' and tokens[1].lower() == 'into':
        table_name = tokens[2]
        data_str = ' '.join(tokens[3:])
        data_parts = data_str.split(';')
        data = {part.split('=')[0].strip(): part.split('=')[1].strip() for part in data_parts}
        db.insert_into(table_name, data)

    elif tokens[0].lower() == 'select' and tokens[1].lower() == 'from':
        table_name = tokens[2]
        conditions, projection, group_by, aggregate, aggregate_column, order_by = {}, None, None, None, None, None
        join_table, left_join_key, right_join_key, join_type = None, None, None, None
        i = 3
        while i < len(tokens):
            if tokens[i].lower() == 'where':
                i += 1
                while i < len(tokens) and '=' in tokens[i]:
                    key, value = tokens[i].split('=')
                    conditions[key] = value.strip(';')
                    i += 1
            elif tokens[i].lower() == 'project':
                    i += 1
                    projection = tokens[i].strip('()').split(',')
                    i += 1
            elif tokens[i].lower() == 'join':
                i += 1
                join_table = tokens[i]
                i += 1
                if i < len(tokens) and tokens[i].lower() == 'on':
                    i += 1
                    join_key = tokens[i]
                    if not '=' in join_key:
                        left_join_key = join_key
                        right_join_key = join_key
                    else:
                        left_join_key = join_key.split("=")[0]
                        right_join_key = join_key.split("=")[1]
                    i += 1
                    if i < len(tokens) and tokens[i].lower() in ['inner', 'left', 'right', 'full']:
                        join_type = tokens[i].lower()
                        i += 1
                    else:
                        join_type = 'inner'  # default as inner
            elif tokens[i].lower() == 'group':
                i += 2
                group_by = tokens[i].strip(';')
                i += 1
                if i < len(tokens) and tokens[i].lower() == 'aggregate':
                    i += 1
                    aggregate_tokens = tokens[i].split('(')
                    aggregate = aggregate_tokens[0].strip()
                    if len(aggregate_tokens) > 1:
                        aggregate_column = aggregate_tokens[1].split(')')[0].strip()
                    i += 1
            elif tokens[i].lower() == 'order':
                i += 2
                order_by = tokens[i].strip(';')
                i += 1
            else:
                i += 1
        
        if join_type:
            result = db.perform_join(table_name, join_table, left_join_key, right_join_key, join_type)
        else:
            result = db.select_from(table_name, conditions, projection, group_by, aggregate, aggregate_column, order_by)
        print_table(result)
    
    elif tokens[0].lower() == 'delete' and tokens[1].lower() == 'from':
        table_name = tokens[2]
        conditions_str = ' '.join(tokens[4:])
        conditions_parts = conditions_str.split('and')
        conditions = {part.split('=')[0].strip(): part.split('=')[1].strip() for part in conditions_parts}
        db.delete_from(table_name, conditions)
    elif tokens[0].lower() == 'update' and tokens[1].lower() == 'table':
        table_name = tokens[2]
        set_index = user_input.lower().index('set')
        where_index = user_input.lower().index('where')
        set_str = user_input[set_index + 3:where_index].strip()
        set_parts = set_str.split(',')
        data = {part.split('=')[0].strip(): part.split('=')[1].strip() for part in set_parts}
        conditions_str = user_input[where_index + 5:].strip()
        conditions_parts = conditions_str.split('and')
        conditions = {part.split('=')[0].strip(): part.split('=')[1].strip() for part in conditions_parts}
        db.update_table(table_name, data, conditions)

    elif tokens[0].lower() == 'exit':
        print("Exiting...")
        break

    else:
        print("Command not recognized.")
    

