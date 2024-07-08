# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 10:30:45 2023

@author: mathieu.olivier
"""
import json
import argparse
import re
from os import listdir
import pandas as pd
from modules.init_db.init_db import initDb, importSrcData, connDb
from utils import utils
from modules.transform.transform import executeTransform, inittable
from modules.export.export import export
from modules.importsource.importSource import decryptFile
from modules.controle.controle import change_type as change_table_column_type,  read_settings

def __main__(args):
    if args.commande == "import":
        importData()
    elif args.commande == "create_csv":
        createCsv()
    elif args.commande == "init_database":
        exeDbInit()
    elif args.commande == "load_csv":
        loadCsvToDb()
        
    elif args.commande =="controle":
        execute_change_type()
    
    elif args.commande == "transform":
        if args.region is None:
            print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif args.region == 0:
            list_region = utils.read_settings('settings/settings.json', "region", "code")
            for r in list_region:
                transform(r)
        else:
            transform(args.region)
    elif args.commande == "export":
        if args.region is None:
            print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif args.region == 0:
            list_region = utils.read_settings('settings/settings.json', "region", "code")
            for r in list_region:
                createExport(r)
        else:
            createExport(args.region)
    elif args.commande == "all":
        allFunctions(args.region)
    return

def exeDbInit():
    dbname = utils.read_settings('settings/settings.json', "db", "name")
    initDb(dbname)
    return

def createCsv():
    paths = utils.get_paths("settings/settings.json", "main")
    allFolders = listdir(paths["input"])
    allFolders.remove('sivss')
    utils.concatSignalement()
    for folderName in allFolders:
        print("loop entrance")
        folderPath = paths["input"] + '/{}'.format(folderName)
        allFiles = listdir(folderPath)
        for inputFileName in allFiles:
            inputFilePath = folderPath + '/' + inputFileName
            outputFilePath = paths["to_csv"] + inputFileName.split('.')[0] + '.csv'
            if re.search('demo.csv|demo.xlsx', inputFileName):
                print('file demo not added')
            elif inputFileName.split('.')[-1].lower() == 'xlsx':
                print(inputFileName)
                utils.convertXlsxToCsv(inputFilePath, outputFilePath)
                print('converted excel file and added: {}'.format(inputFileName))
            elif inputFileName.split('.')[-1].lower() == 'csv':
                outputExcel = inputFilePath.split('.')[0] + '.xlsx'
                df = pd.read_csv(inputFilePath, sep=';', encoding='latin-1', low_memory=False)
                df.to_excel(outputExcel, encoding='UTF-8')
                df2 = pd.read_excel(outputExcel)
                df2.to_csv(outputFilePath, index=None, header=True, sep=';', encoding='UTF-8')
                print('added csv file: {}'.format(inputFileName))
    return

def loadCsvToDb():
    dbname = utils.read_settings('settings/settings.json', "db", "name")
    paths = utils.get_paths("settings/settings.json", "main")
    allCsv = listdir(paths["to_csv"])
    conn = connDb(dbname)
    for inputCsvFilePath in allCsv:
        importSrcData(
            utils.cleanSrcData(
                utils.csvReader(paths["to_csv"] + '/' + inputCsvFilePath)
            ),
            inputCsvFilePath.split('/')[-1].split('.')[0],
            conn,
            dbname
        )
        print("file added to db: {}".format(inputCsvFilePath))
    inittable(conn)
    return

def execute_change_type():
    # Get database name from settings
    settings_file = 'settings/settings.json'
    settings = read_settings(settings_file)
    database = settings['db'][0]['name']

    # Change column types for each table
    from modules.controle.controle import EHPAD_Indicateurs_2021_REG_agg_column_definitions, export_tdbesms_2022_region_agg_column_definitions, commune_2022_region_agg_column_definitions, region_2022_column_definitions

    change_table_column_type(database, 'EHPAD_Indicateurs_2021_REG_agg', EHPAD_Indicateurs_2021_REG_agg_column_definitions)
    change_table_column_type(database, 'export-tdbesms-2022-region-agg', export_tdbesms_2022_region_agg_column_definitions)
    change_table_column_type(database, 'commune_2022', commune_2022_region_agg_column_definitions)
    change_table_column_type(database, 'region_2022', region_2022_column_definitions)

def transform(region):
    # Initialisation des tables si nécessaire
    dbname = utils.read_settings("settings/settings.json", "db", "name")
    conn = connDb(dbname)
    # Extraction des paramètres
    with open('settings/settings.json') as f:
        data = json.load(f)
    
    # Debugging: Afficher le contenu de data
    print("Contenu de data :", data)
    
    # Vérifier si la clé 'parametres' existe dans le JSON
    if "parametres" in data:
        params = data["parametres"]
    else:
        raise KeyError("La clé 'parametres' n'existe pas dans le fichier JSON")
    # Initialisation de la table
    inittable(conn)
    print("Table initialisée avec succès.")
    

    # Exécution de la transformation avec les paramètres
    executeTransform(region)
    print(f"Transformation exécutée pour la région {region}.")


def createExport(region):
    export(region)
    return

def allFunctions(region):
    exeDbInit()
    createCsv()
    loadCsvToDb()
    if region == 0:
        list_region = utils.read_settings('settings/settings.json', "region", "code")
        for r in list_region:
            transform(r)
            createExport(r)
    else:
        transform(region)
        createExport(region)
    return

parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
parser.add_argument("region", type=int, help="Code region pour filtrer")
args = parser.parse_args()

if __name__ == "__main__":
    __main__(args)
