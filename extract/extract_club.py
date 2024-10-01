import requests
import json
import datetime
import os

def club_extract():

    blob = 'clubs'

    api_key = "d9e52390-62a7-46e1-9ac2-3a69df5f3324"
    headers = {"X-AUTH-TOKEN": api_key}
    url = f"https://futdb.app/api/{blob}?page=1"

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    base_path = "/home/fececa/airflow/dags/fifa/data/extract"
    raw_data_path = os.path.join(base_path, "clube/raw")
    images_path = os.path.join(base_path, "clube/imagens")

    file_name = f"{blob}_{current_date}.json"  
    file_path = os.path.join(raw_data_path, file_name)

    os.makedirs(raw_data_path, exist_ok=True)
    os.makedirs(images_path, exist_ok=True)

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
    page_total = obter_total_paginas_api(headers,url)

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
        
        if data_list:
            with open(path, 'w') as file:
                json.dump(data_list, file)
            print(f"Lista de {blob} carregados com o nome: {file_name} no diretório: {raw_data_path}")
        else:
            print(f"Nenhum dado foi carregado para {blob}. O arquivo JSON não foi salvo.")

        return data_list
    data_list = extrair_dados_api(headers,page_total,file_path,blob)

    def imagem_data(headers,data_list,images_path,blob):

        if data_list is None:

            print(f"Nenhuma lista de {blob} encontrada.")

            return

        data_imagens = []

        for blob_item in data_list:

            items = blob_item['items']

            for blob_info in items:

                data_id = blob_info['id']

                column_name = blob_info['name']
                
                url_imagem = f"https://futdb.app/api/{blob}/{data_id}/image"

                response = requests.get(url_imagem, headers=headers)

                if response.status_code == 200:

                    image_path = os.path.join(images_path, f"{data_id}.jpg")  

                    with open(image_path, 'wb') as img_file:
                        img_file.write(response.content) 

                    data_imagens.append({'ID {blob}': data_id, 'Nome {blob}': column_name, 'Caminho do Arquivo': image_path})
                    
                    print(f"Imagem de {column_name} salva no diretório '{image_path}'")  
                
                else:
                    
                    print(f"Erro ao acessar a API para {column_name}: {response.status_code}")  


        return data_imagens
    data_imagens = imagem_data(headers,data_list,images_path,blob)