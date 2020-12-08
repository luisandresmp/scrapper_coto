import requests
from bs4 import BeautifulSoup
from datetime import datetime
import math
import pandas as pd
import os
import time
from progress.bar import ChargingBar

def get_categories():
    url = 'https://www.cotodigital3.com.ar/sitios/cdigi/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    subcategorias = soup.find_all('div', attrs={'class':'g1'})
    all = [i.h2.a.get('href') for i in subcategorias]
    all_2 = [f'https://www.cotodigital3.com.ar{i}' for i in all]
    return(all_2)

def catalogue_leaves(entry):

    list_catalogue=[]    

    # CANTIDAD DE HOJAS DE UNA MISMA CATEGORIA (numero de paginas a scrapear)
    fragment = '?Nf=product.endDate%7CGTEQ+1.6037568E12%7C%7Cproduct.startDate%7CLTEQ+1.6037568E12&No=' 
    url = f'{entry}{fragment}{24}'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    unt = soup.find('p', attrs={'class':'titleSearchResults'})
    unt_c2 = int(unt.span.get_text())
    unt_c3 = math.ceil(int(unt_c2)/24)

    for i in range(0,24*unt_c3,24):
        url = f'{entry}{fragment}{i}'
        response = requests.get(url)
        if response.status_code == 200:
            list_catalogue.append(url)
        else:
            pass
            
    return(list_catalogue) 

def products_per_page(pag): #LISTA DE PRODUCTOS POR PAGINA
    response = requests.get(pag)
    soup = BeautifulSoup(response.text, 'lxml')
    prod = soup.find_all('div', attrs={'class':'product_info_container'})
    all_prod = [prod.a.get('href') for prod in prod]
    all_now = [('https://www.cotodigital3.com.ar'+prod).replace(' ', '')  for prod in all_prod]

    return(all_now)    

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
                name_text = name.get_text()
                coto_product['name'] = " ".join(name_text.split())
                
            # PRECIO DE PRODUCTO
            price = soup_response.find('span', attrs={'class':'atg_store_newPrice'})
            if price is None:
                coto_product['price'] = None
            else:
                price_text = price.get_text()
                price_clean = " ".join(price_text.split())
                coto_product['price'] = price_clean[15:]
                
            # DESCUENTO DEL PRODUCTO
            discount = soup_response.find('span', attrs={'class':'text_price_discount'})
            if discount is None:
                coto_product['discount'] = None 
            else:
                coto_product['discount'] = discount.get_text()
            
            # PRECIO CON DESCUENTO
            discount_price = soup_response.find('span', attrs={'class':'price_discount'})
            if discount_price is None:
                coto_product['discount_price'] = None  
            else:
                coto_product['discount_price'] = discount_price.get_text()
            
            #CUOTAS SIN INTERES
            fees = soup_response.find('div', attrs={'class':'product_discount_pay'})
            if fees is None:
                coto_product['fees'] = None
            else:
                fees_text = fees.get_text()
                coto_product['fees'] = " ".join(fees_text.split()) 
            
            # DATO POR UNIDAD
            unit = soup_response.find('span', attrs={'class':'unit'})
            unit_text = unit.get_text()
            if unit_text:
                coto_product['unit'] = " ".join(unit_text.split()) 
            else:
                coto_product['unit'] = None
            
            # IMAGEN DEL PRODUCTO
            img = soup_response.find('a', attrs={'class':'gall-item'})
            if img:
                coto_product['img'] = img.img.get('src')
            else:
                coto_product['img'] = None

            url = response.request.url
            if url:
                coto_product['url'] = url
            else:
                coto_product['url'] = None

            coto_product['download'] = datetime.now()    
        return coto_product        
            
    except Exception as e:
        pass
        
def load_data(name,data_clean):

    date = datetime.now().strftime('%Y_%m_%d')
    file_name = '{name}_{datetime}.csv'.format(name=name, datetime=date)  

    if os.path.isfile (f'{file_name}'):
        print(f'loading data in {file_name}....\n' )       
        data_clean.to_csv(file_name, mode='a', header=False)
        print('COMPLETED')
    else:
        print(f'\nCreate {file_name}....\n')
        data_clean.to_csv(file_name, encoding='utf-8')
        print('COMPLETED')

def scrapper_strong(option,url):
    print(f'\n\n Starting download of {url}')

    if option == 0:
        option = 'strong_coto'
    else:
        option = 'coto'

    coto = []
    producto_coto=[]
    error = []

    toda_categoria = catalogue_leaves(url)
    if len(toda_categoria)>0:
        print(f'{len(toda_categoria)} pages')
        for i in toda_categoria:
            prod = products_per_page(i)
            coto+=prod
    
    print(f'{len(coto)} products')
    print(f'\n Start of the process: {datetime.now()}\n')
    start = datetime.now()
    bar = ChargingBar('Scrapeando:', max=len(coto))
    for i in coto:
        data = scrapper_coto(i)
        if data:        
            if len(data) == 1:
                error.append(data) 
            else:
                producto_coto.append(data)
        bar.next()
    
    bar.finish() 

    df = pd.DataFrame(producto_coto)
    load_data(option,df)
    if len(error) > 0:
        error_name = 'Error_coto'
        df_error = pd.DataFrame(error)
        load_data(error_name,df_error)
    

    print(f'{len(error)} errors found')
    print(f'{datetime.now()}')
    end = datetime.now()
    print(f'\nEnd of the process: {end - start}\n')   

def validate():
    var_enter = int(input('\n What you want to do? \n\n 0 .-STRONG scrapper coto \n 1 .-CATEGORY scrapper coto \n 2 .-Exit \n\n Only option number --->> '))
    while var_enter > 1:
        if var_enter == 2:
            print('\n Bye bye')
            break
        print('\n Error, enter the number of a valid option')
        var_enter = int(input('\n --->> '))

    return var_enter    

def run():
    option = validate()

    if option == 0:
        categorias = get_categories()
        print(f'\n {len(categorias)} categories found\n')
        for i, category in enumerate(categorias):
            print(f'Scrapeando category {i}/{len(categorias)}')
            scrapper_strong(option,category)
            print(f'Waiting 2 minutes ....\n')
            time.sleep(120)           
    if option == 1:
        category = input('\n\n Enter the category link \n\n (Example: "https://www.cotodigital3.com.ar/sitios/cdigi/browse/catalogo-limpieza-lavandina/_/N-cliwvx")\n\n --->>')
        scrapper_strong(option,category)
    else:
        pass
     
if __name__ == "__main__":
    run()