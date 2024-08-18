import requests

def download_data(url, file_path):
    response = requests.get(url)
    with open(file_path, 'w') as file:
        file.write(response.content.decode('latin1'))

if __name__ == "__main__":
    url = "https://www.gov.br/receitafederal/dados/arrecadacao-estado.csv"
    file_path = f"input/arrecadacao-estado.csv"
    download_data(url, file_path)
