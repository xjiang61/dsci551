import json
import csv
import os

class NoSQLDatabase:
    def __init__(self, data_dir):
        self.data_dir = os.path.abspath(data_dir)
        self.tables = {}

    def create_table(self, table_name: str, columns: list, overwrite_existing=False):
        data_file_path = os.path.join(self.data_dir, f"{table_name.lower()}.json")

        if table_name.lower() in self.tables and not overwrite_existing:
            print(f"Table '{table_name}' already exists.")
            return
        self.tables[table_name.lower()] = {"columns": columns, "data_file": data_file_path}

        # if not overwrite_existing and os.path.exists(data_file_path):
        #     print(f"Data file '{data_file_path}' already exists. Table not overwritten.")
        #     return

        with open(data_file_path, "w") as file:
            json.dump({}, file)

        print("Table created.")

    def load_data(self, table_name, csv_file):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return
        
        csv_file_path = os.path.abspath(csv_file)

        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            data = [{"MovieID": row[0], "YearOfRelease": row[1], "Title": row[2]} for row in reader]

        for entry in data:
            self.insert_into(table_name, entry)


    def insert_into(self, table_name: str, data: dict):
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return

        table = self.tables[table_name]
        data_file = table["data_file"]

        with open(data_file, "r") as file:
            existing_data = json.load(file)

        # Ensure required keys are present in the data
        if all(key.lower() in map(str.lower, data.keys()) for key in ["MovieID", "YearOfRelease", "Title"]):
            # MovieID to int
            movie_id = int(data["MovieID"])

            existing_data[movie_id] = {
                "MovieID": movie_id,
                "YearOfRelease": int(data["YearOfRelease"]),
                "Title": data["Title"]
            }

            with open(data_file, "w") as file:
                json.dump(existing_data, file)

            print("Row inserted.")
        else:
            print("Invalid data format. Please provide MovieID, YearOfRelease, and Title.")


    def select_from(self, table_name: str, conditions: dict = None, projection: list = None,
                    join_table: str = None, join_key: str = None,
                    group_by: str = None, aggregate: str = None, order_by: str = None):
        lowercase_table_name = table_name.lower()

        if lowercase_table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return

        table = self.tables[lowercase_table_name]
        data_file = table["data_file"]

        # Read existing data
        with open(data_file, "r") as file:
            table_data = json.load(file)

        # Apply conditions
        if conditions:
            table_data = {key: value for key, value in table_data.items() if all(value.get(col) == conditions[col] for col in conditions)}

        # Apply join
        if join_table and join_key:
            join_table = join_table.lower()
            join_data_file = self.tables[join_table]["data_file"]
            with open(join_data_file, "r") as file:
                join_data = json.load(file)
            table_data = {key: {**value, **join_data[value[join_key]]} for key, value in table_data.items()}

        # Apply projection
        if projection:
            table_data = {key: {col: value[col] for col in projection} for key, value in table_data.items()}

        # Apply grouping and aggregation
        if group_by and aggregate:
            grouped_data = {}
            for key, value in table_data.items():
                group_key = value[group_by]
                if group_key not in grouped_data:
                    grouped_data[group_key] = []
                grouped_data[group_key].append(value)

            if aggregate.lower() == 'count':
                table_data = {key: len(value) for key, value in grouped_data.items()}
            elif aggregate.lower() == 'sum':
                table_data = {key: sum(v['Amount'] for v in value) for key, value in grouped_data.items()}
            # Add more aggregation functions as needed

        # Apply ordering
        if order_by:
            reverse = False
            if order_by.startswith('-'):
                order_by = order_by[1:]
                reverse = True
            table_data = dict(sorted(table_data.items(), key=lambda x: x[1][order_by], reverse=reverse))

        return table_data


# Example usage
data_directory = "/Users/pyl/Desktop/DSCI551/project/db_data"
db = NoSQLDatabase(data_directory)

# CLI
while True:
    user_input = input("MyDB > ")
    tokens = user_input.split()

    if tokens[0].lower() == 'create' and tokens[1].lower() == 'table':
        table_name = tokens[2]
        columns = tokens[3].strip('()').split(',')
        db.create_table(table_name, columns)

    elif tokens[0].lower() == 'insert' and tokens[1].lower() == 'into':
        table_name = tokens[2]
        data = {}
        for token in tokens[4:]:
            key, value = token.split('=')
            data[key] = value.strip(';')
        db.insert_into(table_name, data)

    elif tokens[0].lower() == 'load_data':
        if len(tokens) == 4 and tokens[2].lower() == 'from':
            table_name = tokens[1].lower()
            csv_file_path = tokens[3]
            if table_name in [name.lower() for name in db.tables]:
                db.load_data(table_name, csv_file_path)
                print("Data loaded into the database.")
            else:
                print(f"Table '{table_name}' does not exist.")
        else:
            print("Invalid 'load_data' command. Usage: load_data <table_name> from <csv_file_path>")

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

        result = db.select_from(table_name, conditions, projection, join_table, join_key, group_by, aggregate, order_by)
        print(result)

    elif tokens[0].lower() == 'exit':
        print("Exiting...")
        break

    else:
        print("Command not recognized.")
