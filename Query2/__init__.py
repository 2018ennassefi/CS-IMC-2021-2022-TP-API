import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver = '{ODBC Driver 17 for SQL Server}'

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]
    if len(server) == 0 or len(database) == 0 or len(username) == 0 or len(password) == 0 or len(
            neo4j_server) == 0 or len(neo4j_user) == 0 or len(neo4j_password) == 0:
        return func.HttpResponse(
            "Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
    dataString = ""
    errorMessage = ""
    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT AVG(averageRating) AS MeanValue, genre from [dbo].[tTitles] as t INNER JOIN[dbo].[tGenres] as g on t.tconst=g.tconst GROUP BY genre")

            rows = cursor.fetchall()
            for row in rows:
                dataString += f"SQL: tconst={row[0]}, primaryTitle={row[1]}, averageRating={row[2]}\n"

    except BaseException:
        errorMessage = "Erreur de connexion a la base SQL"

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
