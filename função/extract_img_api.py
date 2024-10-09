import requests
import os

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