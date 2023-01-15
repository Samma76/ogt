import re
import time
import os
import bs4
import requests
import pandas as pd
import utils
from utils.utils_temperature_process import func_replace, get_temperature

def drop_valid(df: pd.DataFrame, path):
    series_groups = df[['Ec_number', 'enzyme', 'taxonomy_id', 'scientific_name', 'temperature_mid']].groupby(['taxonomy_id'], as_index=False).agg(
        ['count'])
    df_groups = pd.DataFrame(series_groups)
    series_groups = df[['Ec_number', 'enzyme', 'taxonomy_id', 'scientific_name', 'temperature_mid']].groupby(['Ec_number', 'enzyme', 'taxonomy_id', 'scientific_name'], as_index=False)[
        'temperature_mid'].agg(
        ['max', 'min', 'mean'])
    df_groups = pd.DataFrame(series_groups)
    df_groups.rename(columns={'mean': 'Avg_Temperature', 'max': 'Max_Temperature', 'min': 'Min_Temperature'},inplace=True)
    df_groups.reset_index(inplace=True)
    df_groups['Range'] = df_groups['Max_Temperature'] - df_groups['Min_Temperature']
    df_groups.to_excel(path, index=False)

def obtain_enzyme_dataset():
    file1 = open('EC_number.xlsx','r')
    gHeads = {"User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Mobile Safari/537.36"}
    tax_na = re.compile("""Org\('(.*?)'\)">(.*?)</a>""")
    uni_id = re.compile('''<a\shref="javascript:Sequence2\((.*?)\)">''')

    items_right = []
    for each in file1:
        print(each.split('\t')[0])
        if os.path.isfile('enzyme_topt_data/enzyme_topt_{}.xlsx'.format(each.split('\t')[0])) or os.path.isfile('None/enzyme_topt_{}.xlsx'.format(each.split('\t')[0])):
            print("{}_Done".format(each.split('\t')[0]))
            continue
        url = "https://www.brenda-enzymes.org/enzyme.php?ecno={}#TEMPERATURE%20OPTIMUM".format(each.split('\t')[0])
        html = requests.get(url, headers=gHeads).content
        soup = bs4.BeautifulSoup(html, 'html.parser')
        for i in range(0,500):
            temp = soup.find_all('div',{"id":'tab41r{}sr0c0'.format(i)})
            if len(temp) == 0:
                break
            temp = str(temp).split('</span>')[0].split('>')[-1].strip()
            for j in range(0,100):
                name_taxid = soup.find_all('div',{"id":'tab41r{}sr{}c1'.format(i,j)})
                if len(name_taxid) == 0:
                    break
                m1 = re.search(tax_na, str(name_taxid))
                if m1 == None:
                    continue

                uniport_id = soup.find_all('div',{"id":'tab41r{}sr{}c2'.format(i,j)})
                m2 = re.findall(uni_id, str(uniport_id))
                if m2 != None:
                    uniport = m2
                else:
                    uniport = '-'

                print(uniport)

                item_appended = {
                    'enzyme': each.split('\t')[1],
                    'EC_number': each.split('\t')[0],
                    'taxonomy_id': m1.group(1),
                    'scientific_name': m1.group(2),
                    'topt': temp,
                    'uniport_id': uniport
                }
                items_right.append(item_appended)
                #print(item_appended)
        if len(items_right) == 0:
            df = pd.DataFrame([])
            df.to_excel('None/enzyme_topt_{}.xlsx'.format(each.split('\t')[0]), index=False)
            continue
        df = pd.DataFrame([])
        df = df.append(items_right, ignore_index=True)
        df.to_excel('enzyme_topt_data/enzyme_topt_{}.xlsx'.format(each.split('\t')[0]), index=False)
        items_right = []
    file1.close()

def conformity_enzyme_data(filepath1):
    path = filepath1
    files = os.listdir(path)
    items_right = []
    for file in files:
        if not os.path.isdir(file):
            fname = path + "/" + file
            file_data = pd.DataFrame(pd.read_excel(fname))
            # print(file_data)
            for i in range(0, len(file_data)):
                if 'additional information' in str(file_data.loc[i]['topt']):
                    continue
                temperature_mid = get_temperature(file_data.loc[i]['topt'])
                if (float(temperature_mid) > 199) or (float(temperature_mid) < -199):
                    continue
                item_appended = {
                    'Ec_number': file_data.loc[i]['EC_number'],
                    'enzyme': file_data.loc[i]['enzyme'],
                    'taxonomy_id': file_data.loc[i]['taxonomy_id'],
                    'scientific_name': file_data.loc[i]['scientific_name'],
                    'uniport_id': file_data.loc[i]['uniport_id'],
                    'temperature_mid': float(temperature_mid)
                }
                print(item_appended)
                #print('\n')
                items_right.append(item_appended)
    df = pd.DataFrame([])
    df = df.append(items_right, ignore_index=True)
    df.to_excel('enzyme_data.xlsx', index=False)
    print("Done")

obtain_enzyme_dataset()

def combine_all():
    pwd = 'enzyme_topt_data'  # files directory
    file_list = []
    dfs = []
    for root, dirs, files in os.walk(pwd):
    for file in files:
        file_path = os.path.join(root, file)
        file_list.append(file_path)
        df = pd.read_excel(file_path)
        dfs.append(df)
        df = pd.concat(dfs)
        df.to_excel('enzyme_topt_data/result.xls', index=False)

combine_all()