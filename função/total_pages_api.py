import requests

def obter_total_paginas_api(headers,url):


    try:
        response = requests.get(url, headers=headers)

        response.raise_for_status()

        data = response.json()

        page_total = data['pagination']['pageTotal']

        print(f"Número total de páginas: {page_total}")

        return page_total
    
    except requests.exceptions.RequestException as e:

        print(f"Erro ao obter o número total de páginas: {e}")

        return None