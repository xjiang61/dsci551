import json
import os
import pandas as pd
from collections import defaultdict

class Database:
    def __init__(self, data_dir, max_records_per_chunk=1000):
        self.data_dir = data_dir
        self.tables = {}
        self.load_existing_tables()
        self.max_records_per_chunk=max_records_per_chunk

    # CREATE THE TABLE
    def create_table(self, table_name: str, columns: list, overwrite_existing=False):
        table_name_lower = table_name.lower()
        if table_name_lower in self.tables and not overwrite_existing:
            print(f"Table '{table_name}' already exists.")
            return

        # create directory
        data_dir_path = os.path.join(self.data_dir, table_name_lower)
        os.makedirs(data_dir_path, exist_ok=True)

        # create metadata.json 
        metadata_file_path = os.path.join(data_dir_path, "metadata.json")
        with open(metadata_file_path, 'w', encoding='utf-8') as meta_file:
            json.dump({"columns": columns}, meta_file, indent=4)

        # add info to self.tables
        self.tables[table_name_lower] = {"columns": columns, "data_dir": data_dir_path}

        print("Table created.")
    
    # INSERT DATA TO THE SPECIFIED TABLE
    def insert_data(self, table_name: str, values: list):
        table_name_lower = table_name.lower()

        # Check if the table exists
        if table_name_lower not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return

        table_info = self.tables[table_name_lower]
        data_dir = table_info["data_dir"]
        columns = table_info["columns"]
        max_records_per_chunk = self.max_records_per_chunk

        # Create a data dictionary from columns and values
        if len(columns) != len(values):
            print("Error: Number of columns and values does not match.")
            return

        data = dict(zip(columns, values))

        # Find the last chunk file
        files = sorted([f for f in os.listdir(data_dir) if f.endswith('.json') and f.startswith('chunk_')])
        last_file = files[-1] if files else "chunk_0.json"
        last_file_path = os.path.join(data_dir, last_file)

        # Load data from the last chunk
        if os.path.exists(last_file_path):
            with open(last_file_path, 'r', encoding='utf-8') as file:
                chunk_data = json.load(file)
        else:
            chunk_data = []

        # Add new data to the chunk
        if len(chunk_data) >= max_records_per_chunk:
            # Start a new chunk if the last one is full
            chunk_data = [data]
            chunk_count = int(last_file.split('_')[1].split('.')[0]) + 1
            last_file = f"chunk_{chunk_count}.json"
            last_file_path = os.path.join(data_dir, last_file)
        else:
            # Add to the existing chunk
            chunk_data.append(data)

        # Write the updated data back to the chunk file
        with open(last_file_path, 'w', encoding='utf-8') as file:
            json.dump(chunk_data, file, indent=4)

        print(f"Data inserted into table '{table_name}'.")
    

    def batch_insert_data(self, table_name, data):
        if table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        for values in data:
            if len(values) != len(self.tables[table_name]['columns']):
                return 'Number of columns does not match.'
            self.tables[table_name]['data'].append(dict(zip(self.tables[table_name]['columns'], values)))

        self.save_to_file(table_name)
        return 'Batch data inserted successfully.'
   
    def apply_condition(self, record, col_name, operator, value):
        # Check if required columns are present in the record
        if col_name not in record:
            print(f"Error: Column '{col_name}' not found in the record.")
            return None

        record_value = record[col_name]

        # Attempt to convert record_value and value to numbers if they look like numbers
        record_value = self.convert_to_number_if_possible(record_value)
        value = self.convert_to_number_if_possible(value)

        # Perform comparison based on the operator
        if operator == "==":
            return record_value == value
        elif operator == "<":
            return record_value < value
        elif operator == ">":
            return record_value > value
        elif operator == "<=":
            return record_value <= value
        elif operator == ">=":
            return record_value >= value
        elif operator == "!=":
            return record_value != value
    
    def select_data(self, table_name):
        lowercase_table_name = table_name.lower()

        # table existance
        if lowercase_table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return []

        table_info = self.tables[lowercase_table_name]
        data_dir = table_info["data_dir"]
        all_data = []

        # go over all chunks
        for file_name in sorted(os.listdir(data_dir)):
            if file_name.startswith('chunk_') and file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)
                    all_data.extend(chunk_data)

        return all_data
    
    # FETCH ALL RECORDS FROM THE SPECIFIED TABLE WITH THE CONDITION
    def select_data_with_condition(self, table_name, col_name, operator, value):
        lowercase_table_name = table_name.lower()

        # Check if the table exists
        if lowercase_table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return []

        table_info = self.tables[lowercase_table_name]
        data_dir = table_info["data_dir"]
        filtered_data = []

        # Iterate through chunk files
        for file_name in sorted(os.listdir(data_dir)):
            if file_name.startswith('chunk_') and file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)
                    # Apply condition filtering
                    for record in chunk_data:
                        if self.apply_condition(record, col_name, operator, value):
                            filtered_data.append(record)

        return filtered_data

    def select_specific_data_with_condition(self, table_name, col_to_find, col_name, operator, value):
        lowercase_table_name = table_name.lower()

        # Check if the table exists
        if lowercase_table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return []
        
        # Check if the column exists
        if col_to_find not in self.tables[lowercase_table_name]['columns']:
            print(f"Column '{col_to_find}' not found in some records.")
            return []
        if col_name not in self.tables[lowercase_table_name]['columns']:
            print(f"Column '{col_name}' not found in some records.")
            return []

        table_info = self.tables[lowercase_table_name]
        data_dir = table_info["data_dir"]
        filtered_data = []

        # Iterate through chunk files
        for file_name in sorted(os.listdir(data_dir)):
            if file_name.startswith('chunk_') and file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)
                    # Apply condition filtering
                    for record in chunk_data:
                        result = None
                        if self.apply_condition(record, col_name, operator, value):
                            result = {col_to_find: record[col_to_find]}
                        if result is not None:
                            filtered_data.append(result)

        return filtered_data
    
    # DELETE ALL RECORDS FROM THE SPECIFIED TABLE
    def delete_all_records(self, table_name):
        lowercase_table_name = table_name.lower()

        # Check if the table exists
        if lowercase_table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        # Directory path where the table's chunk files are stored
        data_dir = self.tables[lowercase_table_name]["data_dir"]

        # Iterate and delete each chunk file in the table's directory
        for file_name in os.listdir(data_dir):
            if file_name.startswith('chunk_') and file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                os.remove(file_path)

        return f'All records deleted from {table_name}.'

    # DELETE ALL RECORDS FROM THE SPECIFIED TABLE WITH CONDITION
    def delete_records_with_condition(self, table_name, col_name, operator, value):
        lowercase_table_name = table_name.lower()

        # Check if the table exists
        if lowercase_table_name not in self.tables:
            return f'Table {table_name} does not exist.'

        data_dir = self.tables[lowercase_table_name]["data_dir"]

        # Iterate through each chunk file
        for file_name in os.listdir(data_dir):
            if file_name.startswith('chunk_') and file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)

                # Apply condition and filter data
                new_chunk_data = [record for record in chunk_data if not self.apply_condition(record, col_name, operator, value)]
                # Rewrite the chunk file without the deleted records
                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump(new_chunk_data, file, indent=4)

        return f'Records deleted from {table_name} based on the condition.'

    def join_tables(self, table1_name, table2_name, join_column1, join_column2, join_type='inner'):
        if table1_name not in self.tables or table2_name not in self.tables:
            return 'One or both tables do not exist.'

        table1_data_dir = self.tables[table1_name]["data_dir"]
        table2_data_dir = self.tables[table2_name]["data_dir"]
        joined_data = []

        # Process each chunk file of table1
        for file1_name in os.listdir(table1_data_dir):
            if file1_name.startswith('chunk_') and file1_name.endswith('.json'):
                file1_path = os.path.join(table1_data_dir, file1_name)
                with open(file1_path, 'r', encoding='utf-8') as file1:
                    table1_chunk_data = json.load(file1)

                # Process each chunk file of table2 for each chunk of table1
                for file2_name in os.listdir(table2_data_dir):
                    if file2_name.startswith('chunk_') and file2_name.endswith('.json'):
                        file2_path = os.path.join(table2_data_dir, file2_name)
                        with open(file2_path, 'r', encoding='utf-8') as file2:
                            table2_chunk_data = json.load(file2)

                            for row1 in table1_chunk_data:
                                for row2 in table2_chunk_data:
                                    if row1.get(join_column1) == row2.get(join_column2):
                                        combined_row = {**row1, **row2}
                                        joined_data.append(combined_row)

                # Add unmatched rows for left, right, and full joins
                if join_type != 'inner':
                    for row1 in table1_chunk_data:
                        if join_type == 'left' and not any(row1.get(join_column1) == row2.get(join_column2) for row2 in table2_chunk_data):
                            joined_data.append(row1)
                        if join_type == 'full' and not any(row1.get(join_column1) == row2.get(join_column2) for row2 in table2_chunk_data):
                            joined_data.append(row1)

        if join_type in ['right', 'full']:
            for file2_name in os.listdir(table2_data_dir):
                if file2_name.startswith('chunk_') and file2_name.endswith('.json'):
                    file2_path = os.path.join(table2_data_dir, file2_name)
                    with open(file2_path, 'r', encoding='utf-8') as file2:
                        table2_chunk_data = json.load(file2)

                        for row2 in table2_chunk_data:
                            if join_type == 'right' and not any(row2.get(join_column2) == row1.get(join_column1) for row1 in table1_chunk_data):
                                joined_data.append(row2)
                            if join_type == 'full' and not any(row2.get(join_column2) == row1.get(join_column1) for row1 in table1_chunk_data):
                                joined_data.append(row2)

        return joined_data


    def aggregate_data(self, table_name, agg_column, agg_func):
        lowercase_table_name = table_name.lower()

        # Check if the table exists
        if lowercase_table_name not in self.tables:
            return f"Table {table_name} does not exist."

        data_dir = self.tables[lowercase_table_name]["data_dir"]
        aggregated_result = None
        count = 0

        # Iterate through each chunk file
        for file_name in os.listdir(data_dir):
            if file_name.startswith('chunk_') and file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)

                # Check if the aggregation column exists
                if agg_column not in chunk_data[0]:
                    return f"Column {agg_column} does not exist in table {table_name}."

                # Process each row in the chunk
                for row in chunk_data:
                    col_value = self.convert_type_for_comparison(row[agg_column], row[agg_column])
                    if agg_func == 'sum':
                        aggregated_result = aggregated_result + col_value if aggregated_result is not None else col_value
                    elif agg_func == 'avg':
                        aggregated_result = aggregated_result + col_value if aggregated_result is not None else col_value
                        count += 1
                    elif agg_func == 'count':
                        count += 1
                    elif agg_func == 'min':
                        aggregated_result = min(aggregated_result, col_value) if aggregated_result is not None else col_value
                    elif agg_func == 'max':
                        aggregated_result = max(aggregated_result, col_value) if aggregated_result is not None else col_value

        # Final calculation for average
        if agg_func == 'avg':
            return aggregated_result / count if count > 0 else 'No data to calculate average.'

        if agg_func == 'count':
            return count

        return aggregated_result if aggregated_result is not None else 'No data found for aggregation.'
            
    def group_by(self, table_name, group_columns, agg_column, agg_func):
        lowercase_table_name = table_name.lower()

        # Check if the table exists
        if lowercase_table_name not in self.tables:
            return f"Table {table_name} does not exist."

        data_dir = self.tables[lowercase_table_name]["data_dir"]
        grouped_data = defaultdict(list)

        # Iterate through each chunk file
        for file_name in os.listdir(data_dir):
            if file_name.startswith('chunk_') and file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)

                    if group_columns:
                        # Perform grouping based on specified group columns
                        for row in chunk_data:
                            key = tuple(row[col] for col in group_columns)
                            grouped_data[key].append(row)
                    else:
                        # No group by columns, treat entire data set as a single group
                        grouped_data[None].extend(chunk_data)

        # Perform aggregation and format results
        formatted_result = []
        for key, group in grouped_data.items():
            result_dict = {}
            if group_columns:
                result_dict.update({col: key[i] for i, col in enumerate(group_columns)})
            
            if agg_func and agg_column:
                # Filter out rows where agg_column is not a number or is missing
                filtered_data = [self.convert_to_number_if_possible(row.get(agg_column, 0)) for row in group]

                if agg_func.lower() == 'count':
                    result_dict['count'] = len(group)
                elif agg_func.lower() == 'sum':
                    result_dict['sum'] = sum(filtered_data)
                elif agg_func.lower() == 'avg':
                    result_dict['avg'] = sum(filtered_data) / len(filtered_data) if filtered_data else 0
            else:
                # Default to count if no aggregation function is specified
                result_dict['count'] = len(group)

            formatted_result.append(result_dict)

        return formatted_result


    def aggregate_data_internal(self, data, agg_column, agg_func):
        # Filter out rows where agg_column is not a number or is missing
        filtered_data = [row[agg_column] for row in data if agg_column in row and isinstance(row[agg_column], (int, float))]

        if not filtered_data:
            return 'No numeric data found for aggregation.'

        try:
            if agg_func == 'sum':
                return sum(filtered_data)
            elif agg_func == 'avg':
                return sum(filtered_data) / len(filtered_data)
            elif agg_func == 'count':
                # Original 'count' function can count all rows, regardless of the type
                return len(data)
            elif agg_func == 'min':
                return min(filtered_data)
            elif agg_func == 'max':
                return max(filtered_data)
        except Exception as e:
            return f'Aggregation error: {str(e)}'
    
    def order_by(self, data, sort_columns):
        # Return an empty list if no data is provided
        if not data:
            return []

        # Check if all sorting columns exist in the data
        if not all(col in data[0] for col, _ in sort_columns):
            return f"Some columns specified for sorting do not exist in the data."

        # Get the length of the data
        n = len(data)

        # Implementing bubble sort
        for i in range(n):
            for j in range(0, n-i-1):
                # Swap rows based on the columns and order specified in sort_columns
                if self.should_swap(data[j], data[j+1], sort_columns):
                    data[j], data[j+1] = data[j+1], data[j]

        # Return the sorted data
        return data

    def should_swap(self, row1, row2, sort_columns):
        # Compare rows based on each sorting column
        for col, ascending in sort_columns:
            if row1[col] != row2[col]:
                # Determine swap based on ascending or descending order
                return row1[col] > row2[col] if ascending else row1[col] < row2[col]
        return False
    
    def update_records_with_condition(self, table_name, set_col_name, set_value, condition_col_name, condition_value):
        lowercase_table_name = table_name.lower()

        # Check if the table exists
        if lowercase_table_name not in self.tables:
            return f"Table {table_name} does not exist."

        data_dir = self.tables[lowercase_table_name]["data_dir"]

        # Iterate through each chunk file
        for file_name in os.listdir(data_dir):
            if file_name.startswith('chunk_') and file_name.endswith('.json'):
                file_path = os.path.join(data_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    chunk_data = json.load(file)

                # Update records if condition is met
                for record in chunk_data:
                    if record.get(condition_col_name) == condition_value:
                        record[set_col_name] = set_value

                # Save the updated chunk back to the file
                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump(chunk_data, file, indent=4)

        return f"Records updated in {table_name} based on the condition."
    
    # def save_to_file(self, table_name):
    #     filename = f'{table_name}.csv'  # Always use CSV extension
    #     df = pd.DataFrame(self.tables[table_name]['data'])
    #     df.to_csv(filename, index=False)  # Save data to CSV without row indices

    # def update_file_data(self, table_name):
    #     self.save_to_file(table_name)  # Reuse save_to_file for updating

    def load_existing_tables(self):
        for table_name in os.listdir(self.data_dir):
            table_dir_path = os.path.join(self.data_dir, table_name)
            if os.path.isdir(table_dir_path):
                metadata_file_path = os.path.join(table_dir_path, "metadata.json")
                if os.path.exists(metadata_file_path):
                    with open(metadata_file_path, "r", encoding='utf-8') as file:
                        metadata = json.load(file)
                        self.tables[table_name] = {
                            "columns": metadata["columns"],
                            "data_dir": table_dir_path
                        }
                else:
                    print(f"Metadata file not found for table '{table_name}'.")


    # def load_from_file(self, table_name):
    #     filename = f'{table_name}.csv'
    #     try:
    #         df = pd.read_csv(filename)
    #         data = {'columns': list(df.columns), 'data': df.to_dict(orient='records')}
    #         self.tables[table_name] = data
    #     except FileNotFoundError:
    #         print(f"File {filename} not found.")
    #     except pd.errors.EmptyDataError:
    #         print(f"Error loading data from {filename}.")


    def execute_query(self, query):
        tokens = query.lower().split()
        table_name = None
        selected_data = None

        # whether exit
        if query.strip().lower() == 'exit':
            return 'Exiting...'

        # find specific table
        if 'from' in tokens:
            from_index = tokens.index('from')
            table_name = tokens[from_index + 1]
        elif query.startswith('create table') or query.startswith('insert into'):
            table_name = query.split()[2]

        if query.startswith('create table'):
            tokens = query.split()
            table_name = tokens[2]
            columns = [col.strip(',()') for col in tokens[3:]]
            result = self.create_table(table_name, columns)
            return result  # Return the result directly, which may be "Table created" or "Table already exists."
        
        elif query.startswith('insert into'):
            tokens = query.split(maxsplit=3)
            table_name = tokens[2]

            values_part = tokens[3].strip("()")  
            values = [val.strip(' " ') for val in values_part.split(',')] 

            return self.insert_data(table_name, values)


        elif query.startswith('find all'):
            tokens = query.split()
            table_name = tokens[2]
            if(len(tokens) == 3) or ('order by' in query and not 'where' in query):
                selected_data = self.select_data(table_name)
            elif 'where' in tokens and len(tokens) > 3:
                # Find statement with conditions
                condition_start_index = tokens.index('where')
                condition_tokens = tokens[condition_start_index + 1:]

                # Ensure the condition has a valid structure
                if len(condition_tokens) == 3:
                    col_name = condition_tokens[0]
                    operator = condition_tokens[1]
                    value = condition_tokens[2].strip('\"')
                    try:
                        if col_name in self.tables[table_name]['columns']:
                            value = self.convert_type(self.tables[table_name], col_name, value)
                    except ValueError:
                        return 'Invalid value type.'

                    selected_data = self.select_data_with_condition(table_name, col_name, operator, value)
                    # return selected_data
                else:
                    return 'Invalid condition format. Use: find all <table_name> where <col_name> <operator> "<value>"'

            
        elif query.startswith('delete all'):
            tokens = query.split()
            table_name = tokens[2]

            if len(tokens) == 3:
                return self.delete_all_records(table_name)
            
            elif len(tokens) > 3 and tokens[3] == 'where':
                # Delete with condition
                condition_start_index = tokens.index('where')
                condition_tokens = tokens[condition_start_index + 1:]

                # Ensure the condition has a valid structure
                if len(condition_tokens) == 3:
                    col_name = condition_tokens[0]
                    operator = condition_tokens[1]
                    value = condition_tokens[2].strip('\"')

                    # Perform the deletion based on the condition
                    return self.delete_records_with_condition(table_name, col_name, operator, value)

                else:
                    return 'Invalid condition format. Use: delete all <table_name> where <col_name> <operator> "<value>"'
            
            else:
                return 'Invalid delete command format. Use: delete all <table_name> [where <col_name> <operator> "<value>"]'
            
        elif query.startswith('find'):
            tokens = query.split()
            col_to_find = tokens[1]  
            table_name = tokens[3] 
            if table_name not in self.tables:
                return f"Table '{table_name}' does not exist."

            if len(tokens) > 4:
                condition_start_index = tokens.index('where')
                condition_tokens = tokens[condition_start_index + 1:]

                if len(condition_tokens) >= 3:
                    col_name = condition_tokens[0]
                    operator = condition_tokens[1]
                    value = " ".join(condition_tokens[2:]).strip('\"')
                    try:
                        if col_name in self.tables[table_name]['columns']:
                            value = self.convert_type(self.tables[table_name], col_name, value)
                    except ValueError:
                        return 'Invalid value type.'

                    selected_data = self.select_specific_data_with_condition(table_name, col_to_find, col_name, operator, value)
                    # return selected_data
            else:
                return 'Invalid condition format. Use: find <column_name> from <table_name> where <col_name> <operator> "<value>"'
        
        elif query.startswith('delete from'):
            tokens = query.split()
            table_name = tokens[2]

            if 'where' in tokens:
                where_index = tokens.index('where')
                col_name = tokens[where_index + 1]
                operator = tokens[where_index + 2]
                value = tokens[where_index + 3].strip('\"')

                return self.delete_records_with_condition(table_name, col_name, operator, value)
            else:
                return self.delete_all_records(table_name)

        elif query.startswith('update'):
            try:
                tokens = query.split()
                table_name = tokens[1]

                set_index = tokens.index('set')
                set_col_name = tokens[set_index + 1]
                set_value_str = query.split('set')[1].split('where')[0]
                set_value = set_value_str.split('=')[1].strip(' \'\"')

                where_index = tokens.index('where')
                condition_str = query.split('where')[1]
                condition_col_name = condition_str.split('=')[0].strip()
                condition_value = condition_str.split('=')[1].strip(' \'\"')

                return self.update_records_with_condition(table_name, set_col_name, set_value, condition_col_name, condition_value)
            except IndexError:
                return 'Error in parsing the update query.'

        elif query.startswith('join'):
            tokens = query.split()
            if len(tokens) < 6:  # Adjusted for two join columns
                return 'Invalid join query format.'

            table1_name = tokens[1]
            table2_name = tokens[2]
            join_column1 = tokens[4]  # Join column for the first table
            join_column2 = tokens[5]  # Join column for the second table
            join_type = 'inner'  # Default join type

            # Handle different types of joins if specified in the query
            if 'left' in query:
                join_type = 'left'
            elif 'right' in query:
                join_type = 'right'
            elif 'full' in query:
                join_type = 'full'

            return self.join_tables(table1_name, table2_name, join_column1, join_column2, join_type)
        
        
        elif 'select' in tokens and 'group by' in query.lower():
            select_parts = query.lower().split('select')[1].split('from')[0].strip().split(',')
            agg_func, agg_column = None, None
            if any(func in select_parts[0] for func in ['sum', 'avg', 'count', 'min', 'max']):
                agg_func_part = select_parts[0].split('(')
                agg_func = agg_func_part[0].strip()
                agg_column = agg_func_part[1].split(')')[0].strip()

            group_by_index = query.lower().find('group by')
            group_columns = query[group_by_index + 9:].split(',')

            return self.group_by(table_name, group_columns, agg_column, agg_func)
        else:
            return 'Unsupported query.'

        # handle order by query
        if 'order by' in query.lower():
            order_by_index = query.lower().find('order by')
            order_by_clause = query[order_by_index + 9:].strip()

            order_columns = order_by_clause.split(',')
            sort_columns = []
            for col in order_columns:
                col_parts = col.strip().split()
                column_name = col_parts[0].strip()
                ascending = True 
                if len(col_parts) > 1 and col_parts[1].lower() == 'desc':
                    ascending = False
                sort_columns.append((column_name, ascending))
            
            return self.order_by(selected_data, sort_columns)
        
        return selected_data

        
        
        
    def convert_type(self, table, col_name, value):
        if value.isdigit():
            return int(value)
        else:
            return value
    
    def convert_type_for_comparison(self, col_value, value):
        if isinstance(col_value, int):
            return int(value)
        elif isinstance(col_value, float):
            return float(value)
        else:
            return value
        
    def format_select_output(self, row):
        return ", ".join(f"{key}: {value}" for key, value in row.items())
    
    def check_condition(self, row, col_name, operator, value):
        if col_name in row:
            col_value = row[col_name]

            if operator == '=' and col_value == value:
                return True
            elif operator == '>' and col_value > value:
                return True
            elif operator == '<' and col_value < value:
                return True

        return False

    def convert_to_number_if_possible(self, value):
            try:
                return float(value)
            except ValueError:
                return value

def print_table(data):
    if not data:
        return

    # Collect all unique column names from all rows
    columns = set(col for row in data for col in row)
    columns = sorted(list(columns))  # Sort for consistent order

    # Calculate the maximum width for each column
    col_widths = {col: max(len(str(row.get(col, ''))) for row in data) for col in columns}
    col_widths = {col: max(len(col), col_widths[col]) for col in columns}

    # Print the table header
    header = " | ".join(f"{col.ljust(col_widths[col])}" for col in columns)
    print(header)
    print("-" * len(header))

    # Print each row of the table
    for row in data:
        print(" | ".join(f"{str(row.get(col, '')).ljust(col_widths[col])}" for col in columns))

if __name__ == "__main__":

    db = Database('./data')
    while True:
        user_input = input('MyDB > ')
        result = db.execute_query(user_input)

        if result == 'Exiting...':
            break

        if isinstance(result, list):
            print_table(result)
        elif result:
            print(result)