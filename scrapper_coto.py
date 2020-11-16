import requests
from bs4 import BeautifulSoup
from datetime import datetime
import math
import pandas as pd
import os
import time
from progress.bar import ChargingBar

def obtener_categorias():
    url = 'https://www.cotodigital3.com.ar/sitios/cdigi/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    subcategorias = soup.find_all('div', attrs={'class':'g1'})
    todas = [hola.h2.a.get('href') for hola in subcategorias]
    todas2 = [f'https://www.cotodigital3.com.ar{cat}' for cat in todas]
    return(todas2)

def hojas_del_catalogo(entrada):

    lista=[]    

    # CANTIDAD DE HOJAS DE UNA MISMA CATEGORIA (numero de paginas a scrapear)
    fragmento = '?Nf=product.endDate%7CGTEQ+1.6037568E12%7C%7Cproduct.startDate%7CLTEQ+1.6037568E12&No=' 
    url = f'{entrada}{fragmento}{24}'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    unt = soup.find('p', attrs={'class':'titleSearchResults'})
    unt_c2 = int(unt.span.get_text())
    unt_c3 = math.ceil(int(unt_c2)/24)

    for i in range(0,24*unt_c3,24):
        url = f'{entrada}{fragmento}{i}'
        response = requests.get(url)
        if response.status_code == 200:
            lista.append(url)
        else:
            pass
            
    return(lista) 

def productos_por_pagina(pag): #LISTA DE PRODUCTOS POR PAGINA
    response = requests.get(pag)
    soup = BeautifulSoup(response.text, 'lxml')
    prod = soup.find_all('div', attrs={'class':'product_info_container'})
    todos_prod = [prod.a.get('href') for prod in prod]
    todos_ahora = [('https://www.cotodigital3.com.ar'+prod).replace(' ', '')  for prod in todos_prod]
    #print(todos_ahora)
    return(todos_ahora)    

def scrapper_coto(url):
    try:
        response = requests.get(url)
        coto_product = {}
        if response.status_code == 200:
            soup_response = BeautifulSoup(response.text, 'lxml')
            
            # NOMBRE & ID DEL PRODUCTO
            name = soup_response.find('h1', attrs={'class':'product_page'})
            if name is None:
                coto_product['name'] = None
            else:
                #print(name)
                name_text = name.get_text()
                coto_product['name'] = " ".join(name_text.split())
                #print('nombre')

            # PRECIO DE PRODUCTO
            price = soup_response.find('span', attrs={'class':'atg_store_newPrice'})
            if price is None:
                coto_product['price'] = None
            else:
                price_text = price.get_text()
                price_clean = " ".join(price_text.split())
                coto_product['price'] = price_clean[15:]
                #print('precio')

            # DESCUENTO DEL PRODUCTO
            discount = soup_response.find('span', attrs={'class':'text_price_discount'})
            #print (discount)
            if discount is None:
                coto_product['discount'] = None 
            else:
                coto_product['discount'] = discount.get_text()
                #print('descuento')

            # PRECIO CON DESCUENTO
            discount_price = soup_response.find('span', attrs={'class':'price_discount'})
            if discount_price is None:
                coto_product['discount_price'] = None  
                #print('precio descuento')   
            else:
                coto_product['discount_price'] = discount_price.get_text()
                #print('precio descuento')    

            #CUOTAS SIN INTERES
            fees = soup_response.find('div', attrs={'class':'product_discount_pay'})
            if fees is None:
                coto_product['fees'] = None
                #print('cuotas')      
            else:
                fees_text = fees.get_text()
                coto_product['fees'] = " ".join(fees_text.split()) 
                #print('cuotas') 

            # DATO POR UNIDAD
            unit = soup_response.find('span', attrs={'class':'unit'})
            unit_text = unit.get_text()
            if unit_text:
                coto_product['unit'] = " ".join(unit_text.split()) 
                #print('unidad')       
            else:
                coto_product['unit'] = None
                #print('unidad')
            
            # IMAGEN DEL PRODUCTO
            img = soup_response.find('a', attrs={'class':'gall-item'})
            if img:
                coto_product['img'] = img.img.get('src')
                #print('imagen')
            else:
                coto_product['img'] = None

            url = response.request.url
            if url:
                coto_product['url'] = url
            else:
                coto_product['url'] = None

            coto_product['download'] = datetime.now()    
        #print(coto_product)    
        return coto_product        
            
    except Exception as e:
        coto_product = {}
        coto_product['url'] = url
        #pass
        return coto_product    

def load_data(name,data_clean):

    if os.path.isfile ('db_coto.csv'):
        print('loading data....\n')
        data_clean.to_csv('db_coto.csv', mode='a', header=False)
        print('COMPLETED')
    else:
        print(f'\nCreate db_coto.csv....\n')
        data_clean.to_csv('db_coto.csv', encoding='utf-8')
        print('COMPLETED')


def categoria_hard_delay(url):
    print(f'Iniciando descarga de {url}')
    #prueba= 'https://www.cotodigital3.com.ar/sitios/cdigi/browse/catalogo-limpieza-lavandina/_/N-cliwvx'
    coto = []
    producto_coto=[]
    error = []

    toda_categoria = hojas_del_catalogo(url)
    if len(toda_categoria)>0:
        print(f'se encontraron {len(toda_categoria)} paginas')
        for i in toda_categoria:
            prod = productos_por_pagina(i)
            coto+=prod
    
    print(f'se encontraron {len(coto)} productos')
    print(f'\nInicio del proceso: {datetime.now()}\n')
    start = datetime.now()
    bar = ChargingBar('Scrapeando:', max=len(coto))
    for i in coto:
        data = scrapper_coto(i)
        producto_coto.append(data)
        if len(data) == 1:
           error.append(data) 
        else:   
            pass
        bar.next()
    
    bar.finish() 

    #print(len(toda_categoria))
    df = pd.DataFrame(producto_coto)
    load_data(url,df)
    #print(len(coto))
    print(f'se encontraron {len(error)} errores')
    print(f'{datetime.now()}')
    end = datetime.now()
    print(f'\nFin del proceso: {end - start}\n')
    print(f'Esperando 2 minutos ....\n')
    time.sleep(120)

def validar():
    var_onu = int(input('\n Desea hacer un: \n\n 0 .-HARD scrapper coto \n 1 .-HIGH scrapper coto \n 2 .-Exit \n\n --->> '))
    while var_onu > 1:
        if var_onu == 2:
            print('\n Bye bye')
            break
        print('\n Error, debe introducir el numero de una opcion valida')
        var_onu = int(input('\n --->> '))

    return var_onu    



def run():
    #categorias = obtener_categorias()
    
    hola = validar()
    print(hola)
    

    #print(categorias[83:])
    #for i in categorias[83:]:
    #    categoria_hard_delay(i)
 
    #quepaso = scrapper_coto('https://www.cotodigital3.com.ar/sitios/cdigi/producto/-lavandina-en-gel-vim-original-700-ml/_/A-00478108-00478108-200')     
    #print(quepaso)  
     
   
if __name__ == "__main__":
    run()