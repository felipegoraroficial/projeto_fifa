import datetime
import requests
import json

def extrair_dados_api(headers,page_total,path,blob):

    page_total

    if page_total is None:

        page_total = 1

    data_list = []  

    for i in range(1, page_total + 1):

        url = f"https://futdb.app/api/{blob}?page={i}"
        try:
            response = requests.get(url, headers=headers)

            response.raise_for_status()

            data = response.json()

            data_list.append(data)


        except requests.exceptions.RequestException as e:

            print(f"Erro ao obter dados da página {i}: {e}")

    with open(path, 'w') as file:
        json.dump(data_list, file)
    print(f"Lista de {blob} carregados no caminho: {path}")

    return data_list