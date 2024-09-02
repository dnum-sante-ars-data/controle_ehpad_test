import json
import argparse
import re
from os import listdir
import pandas as pd
from modules.init_db.init_db import initDb, importSrcData, connDb, createTablesWithTypes, modifier_finess, tableExists
from utils import utils
from modules.transform.transform import executeTransform, inittable
from modules.importsource.importSource import decryptFile
import os

def read_settings():
    with open('settings/settings.json') as f:
        data = json.load(f)
    if "parametres" in data:
        return data["parametres"], data
    else:
        raise KeyError("La clé 'parametres' n'existe pas dans le fichier JSON")

def main(args):
    if args.commande == "import":
        importData(args.projet)
    elif args.commande == "create_csv":
        createCsv(args.projet)
    elif args.commande == "init_database":
        exeDbInit(args.projet)
    elif args.commande == "load_csv":
        loadCsvToDb(args.projet)
    elif args.commande == "transform":
        if args.region is None:
            print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif args.region == 0:
            list_region = utils.read_settings('settings/settings.json', "region", "code")
            for r in list_region:
                transform(r, args.projet)
        else:
            transform(args.region, args.projet)
    elif args.commande == "export":
        if args.region is None:
            print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif args.region == 0:
            list_region = utils.read_settings('settings/settings.json', "region", "code")
            for r in list_region:
                createExport(r, args.projet)
        else:
            createExport(args.region, args.projet)
    elif args.commande == "all":
        allFunctions(args.region, args.projet)
    return

def exeDbInit(projet):
    dbname = utils.read_settings('settings/settings.json', "db", "name")

    # Définir le nom de la table où les données seront insérées
    table_name = "finess.csv"
    
    # Connexion à la base de données SQLite
    conn = connDb(dbname)
    params, data = read_settings()
    
    input_path = r"C:\Users\farizh.sampebgo\Desktop\Matrice pré_ciblage\controle_ehpad\data\input\finess\finess.csv"
    output_path = r"C:\Users\farizh.sampebgo\Desktop\Matrice pré_ciblage\controle_ehpad\data\to_csv"
    colonne_names = [
        "Section : equipementsocial", "Numéro FINESS ET", "Code de la discipline d'équipement",
        "Libellé de la discipline d'équipement", "Code d'activité", "Libellé du code d'activité", "Code clientèle",
        "Libellé du code clientèle", "Code source information", "Capacité installée totale",
        "Capacité installation hommes", "Capacité installation femmes", "Capacité installation habilités aide sociale",
        "Age minimum installation", "Age maximum installation", "Indicateur de suppression de l’installation (O/N)",
        "Date de dernière installation", "Date de première autorisation", "Capacité autorisée totale", "Capacité autorisation hommes",
        "Capacité autorisation femmes", "Capacité autorisation habilités aide sociale", "Age minimum autorisation",
        "Age maximum autorisation", "Indicateur de suppression de l’autorisation (O/N)", "Date d'autorisation",
        "Date MAJ de l’autorisation", "Date MAJ de l’installation"
    ]
    
    # Appeler la fonction modifier_finess pour modifier le fichier et le charger dans SQLite
    modifier_finess(input_path, output_path, colonne_names, dbname, table_name)
    
    createTablesWithTypes(conn, data)
    conn.close()
    return


def createCsv(projet):
    allFolders = listdir('data/input')
    for folderName in allFolders:
        folderPath = f'data/input/{folderName}'
        allFiles = listdir(folderPath)
        for inputFileName in allFiles:
            inputFilePath = os.path.join(folderPath, inputFileName)
            outputFilePath = os.path.join('data/to_csv', f'{inputFileName.split(".")[0]}.csv')
            if re.search('demo.csv|demo.xlsx', inputFileName):
                print('file demo not added')
            elif inputFileName.lower().endswith('.xlsx'):
                utils.convertXlsxToCsv(inputFilePath, outputFilePath)
                print(f'converted excel file and added: {inputFileName}')
            elif inputFileName.lower().endswith('.csv'):
                outputExcel = f'{inputFilePath.split(".")[0]}.xlsx'
                df = pd.read_csv(inputFilePath, sep=';', encoding='latin-1', low_memory=False)
                df.to_excel(outputExcel, index=None, header=True, encoding='UTF-8')
                df2 = pd.read_excel(outputExcel)
                df2.to_csv(outputFilePath, index=None, header=True, sep=';', encoding='UTF-8')
                print(f'added csv file: {inputFileName}')

def loadCsvToDb(projet):
    dbname = utils.read_settings('settings/settings.json', "db", "name")
    allCsv = listdir('data/to_csv')
    conn = connDb(dbname)
    _, data = read_settings()
    for inputCsvFilePath in allCsv:
        table_name = inputCsvFilePath.split('/')[-1].split('.')[0]
        if tableExists(conn, table_name):
            print(f"La table {table_name} existe déjà. Ajout des données sans modification du type.")
            importSrcData(
                utils.cleanSrcData(
                    utils.csvReader('data/to_csv/' + inputCsvFilePath)
                ),
                table_name,
                conn
            )
        else:
            print(f"La table {table_name} n'existe pas. Création de la table et ajout des données.")
            createTablesWithTypes(conn, data)
            importSrcData(
                utils.cleanSrcData(
                    utils.csvReader('data/to_csv/' + inputCsvFilePath)
                ),
                table_name,
                conn
            )
        print("file added to db: {}".format(inputCsvFilePath))
    conn.close()
    return

def transform(region, projet):
    dbname = utils.read_settings("settings/settings.json", "db", "name")
    conn = connDb(dbname)
    params, _ = read_settings()
    inittable(conn)
    print("Table initialisée avec succès.")
    executeTransform(region, projet)
    print(f"Transformation exécutée pour la région {region}.")
    conn.close()

def allFunctions(region, projet):
    exeDbInit(projet)
    #loadCsvToDb(projet)
    
    if region == 0:
        list_region = utils.read_settings('settings/settings.json', "region", "code")
        print(f"Liste des régions lues : {list_region}")
        for r in list_region:
            print(f"Traitement de la région : {r}")
            transform(r, projet)
    else:
        print(f"Traitement de la région spécifiée : {region}")
        transform(region, projet)
    
    return

parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
parser.add_argument("--projet", type=str, required=True, help="Projet à exécuter (ESMS ou PH)")
parser.add_argument("--region", type=int, required=True, help="Code région pour filtrer")
args = parser.parse_args()

if __name__ == "__main__":
    main(args)
