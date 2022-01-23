import logging
from py2neo import Graph
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    genre = req.params.get('genre')
    if not genre:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            genre = req_body.get('genre')

    actor = req.params.get('actor')
    if not actor:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            actor = req_body.get('actor')

    director = req.params.get('director')
    if not director:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            director = req_body.get('director')
    
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0 or len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        cypher_query = "MATCH (t:Title)"
        if len(genre):
            cypher_query += ' (t)-[:IS_OF_GENRE]->(g: Genre {{label: "{0}"}})'.format(genre)
        if len(actor):
            cypher_query += ' ( :Name {{primaryName:"{0}"}})-[:ACTED_IN]->(t)'.format(actor)
        if len(director):
            cypher_query += ' ( :Name {{primaryName:"{0}"}})-[:DIRECTED]->(t)'.format(director)
        cypher_query += ' RETURN COLLECT(t.tconst);'

        movies_ids = graph.run(cypher_query)

        formatted_ids = tuple(set(movies_ids))

        try:
            logging.info("Test de connexion avec pyodbc...")
            with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT AVG(runtimeMinutes) FROM [dbo].[tTitles] WHERE tconst IN {0}".format(formatted_ids))

                result = cursor.fetchall()
                dataString += f"La durée moyenne des films est {result}\n"

        except:
            errorMessage = "Erreur de connexion a la base SQL"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"

    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + " Connexions réussies a Neo4j et SQL!")
