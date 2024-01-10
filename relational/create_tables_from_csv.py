import csv, json
import os

def split_csv_into_chunks(csv_file_path, output_dir, max_records_per_chunk=1000):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(csv_file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)

        # Store header info in metadata.json
        headers = csv_reader.fieldnames
        metadata_file_path = os.path.join(output_dir, "metadata.json")
        with open(metadata_file_path, 'w', encoding='utf-8') as meta_file:
            json.dump({"columns": headers}, meta_file, indent=4)

        chunk_count = 0
        current_chunk = []

        for row in csv_reader:
            current_chunk.append(row)

            if len(current_chunk) >= max_records_per_chunk:
                current_chunk.sort(key=lambda x: x[csv_reader.fieldnames[0]])
                write_chunk(current_chunk, output_dir, chunk_count)
                chunk_count += 1
                current_chunk = []

        if current_chunk:
            current_chunk.sort(key=lambda x: x[csv_reader.fieldnames[0]])
            write_chunk(current_chunk, output_dir, chunk_count)


def write_chunk(chunk, output_dir, chunk_count):
    chunk_file_name = f"chunk_{chunk_count}.json"
    chunk_file_path = os.path.join(output_dir, chunk_file_name)
    with open(chunk_file_path, 'w', encoding='utf-8') as file:
        json.dump(chunk, file, indent=4)
    print(f"Written {chunk_file_name}")


def process_all_csv_files(dir_path):
    if not os.path.exists(dir_path):
        print(f"Directory {dir_path} does not exist.")
        return

    for file_name in os.listdir(dir_path):
        if file_name.endswith('.csv'):
            csv_file_path = os.path.join(dir_path, file_name)
            output_dir = os.path.join('./data', os.path.splitext(file_name)[0])
            split_csv_into_chunks(csv_file_path, output_dir)
            print(f"Processed {file_name}")


dir_path = './data1' 
process_all_csv_files(dir_path)