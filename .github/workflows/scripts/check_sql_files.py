import argparse

def check_sql_files(file):
    found_staging = False
    if file.endswith(".sql"):
        with open(file, "r") as f:
            lines = f.readlines()
            for line in lines:
                if "basedosdados-dev" in line:
                    found_staging = True
                    print(f"Found 'basedosdados-dev' in {file}")
                    break
    return found_staging

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check for 'basedosdados-dev' occurrences in SQL files.")
    parser.add_argument("file", help="Path to the SQL file to check")
    args = parser.parse_args()

    if check_sql_files(args.file):
        exit(1)
    else:
        print("No occurrences of 'basedosdados-dev' found in SQL files.")