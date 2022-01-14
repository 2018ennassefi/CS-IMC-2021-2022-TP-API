import logging, os
from py2neo import Graph

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
    errorMessage = ""
    dataString = ""

    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        names = graph.run(
            "MATCH (n:Name)-[r]->() WITH n, COUNT(DISTINCT TYPE(r)) as roles WHERE roles>2 RETURN n.nconst, n.primaryName LIMIT 10;")
        for producer in names:
            dataString += f"CYPHER: nconst={producer['n.nconst']}, primaryName={producer['n.primaryName']}\n"

    except:
        errorMessage = "Erreur de connexion a la base Neo4j"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + " Connexions réussies a Neo4j et SQL!")
