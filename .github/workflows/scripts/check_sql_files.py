import argparse
import os

def check_sql_files(file):
    found_staging = False
    if os.path.exists(file) and file.endswith(".sql"):
        with open(file, "r") as f:
            lines = f.readlines()
            for line in lines:
                if "basedosdados-staging" in line:
                    found_staging = True
                    print(f"Found 'basedosdados-staging' in {file}")
                    break
    return found_staging

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check for 'basedosdados-staging' occurrences in SQL files.")
    parser.add_argument("file", help="Path to the SQL file to check")
    args = parser.parse_args()

    if check_sql_files(args.file):
        exit(1)
    else:
        print("No occurrences of 'basedosdados-staging' found in SQL files.")
