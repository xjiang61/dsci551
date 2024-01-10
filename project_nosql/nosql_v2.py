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

        # # overwrite_existing is true doesn't work
        # if not overwrite_existing and os.path.exists(data_file_path):
        #     print(f"Data file '{data_file_path}' already exists. Table not overwritten.")
        #     return

        with open(data_file_path, "w") as file:
            json.dump([current], file)

        print("Table created.")

    # def load_data(self, table_name):
    #     if table_name not in self.tables:
    #         print(f"Table '{table_name}' does not exist.")
    #         return
        
    #     csv_file_path = os.path.abspath(csv_file)

    #     with open(csv_file, 'r') as file:
    #         reader = csv.reader(file)
    #         data = [{"MovieID": row[0], "YearOfRelease": row[1], "Title": row[2]} for row in reader]

    #     for entry in data:
    #         self.insert_into(table_name, entry)


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
                    group_by: str = None, aggregate: str = None, order_by: str = None):
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

        # Apply grouping and aggregation
        if group_by and aggregate:
            grouped_data = {}
            for record in aggregated_data:
                group_key = record[group_by]
                if group_key not in grouped_data:
                    grouped_data[group_key] = []
                grouped_data[group_key].append(record)

            if aggregate.lower() == 'count':
                aggregated_data = {key: len(value) for key, value in grouped_data.items()}
            elif aggregate.lower() == 'sum':
                aggregated_data = {key: sum(item['Amount'] for item in value) for key, value in grouped_data.items()}
            # Add more aggregation functions as needed

        # Apply ordering
        if order_by:
            reverse = order_by.startswith('-')
            sort_key = order_by[1:] if reverse else order_by
            aggregated_data = sorted(aggregated_data, key=lambda x: x[sort_key], reverse=reverse)

        return aggregated_data



def print_table(data):
    if not data:
        print("No data to display.")
        return

    columns = data[0].keys()
    col_width = {col: max(len(col), max(len(str(row[col])) for row in data)) for col in columns}

    header = ' | '.join(f"{col.ljust(col_width[col])}" for col in columns)
    print(header)
    print('-' * len(header))

    for row in data:
        print(' | '.join(f"{str(row[col]).ljust(col_width[col])}" for col in columns))

# Example usage
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
        conditions, projection, join_table, join_key, group_by, aggregate, order_by = {}, None, None, None, None, None, None

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
                    join_key = tokens[i].strip(';')
                    i += 1
            elif tokens[i].lower() == 'group':
                i += 2
                group_by = tokens[i].strip(';')
                i += 1
                if i < len(tokens) and tokens[i].lower() == 'aggregate':
                    i += 1
                    aggregate = tokens[i].strip(';')
                    i += 1
            elif tokens[i].lower() == 'order':
                i += 2
                order_by = tokens[i].strip(';')
                i += 1
            else:
                i += 1

        result = db.select_from(table_name, conditions, projection, group_by, aggregate, order_by)
        print_table(result)

    elif tokens[0].lower() == 'exit':
        print("Exiting...")
        break

    else:
        print("Command not recognized.")
    

