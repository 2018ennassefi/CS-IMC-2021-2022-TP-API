import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver = '{ODBC Driver 17 for SQL Server}'

    if len(server) == 0 or len(database) == 0 or len(
            username) == 0 or len(password) == 0:
        return func.HttpResponse(
            "Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
    dataString = ""
    errorMessage = ""
    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT genre from [dbo].[tGenres] WHERE tconst in (\
                    SELECT tconst\
                    FROM[dbo].[tTitles]\
                    WHERE tconst in (\
                        SELECT tconst\
                        FROM[dbo].[tPrincipals]\
                        GROUP BY tconst, nconst\
                        HAVING(COUNT(DISTINCT category) > 1)\
                    ))")

            rows = cursor.fetchall()
            dataString += "Les genres pour lesquels au moins un film a une même personne qui a été la fois directeur et acteur sont:"
            for row in rows:
                dataString += f"SQL: genre={row[0]}\n"

    except BaseException:
        errorMessage = "Erreur de connexion a la base SQL"

    if errorMessage != "":
        return func.HttpResponse(
            dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(
            dataString + " Connexions réussies a SQL!")
