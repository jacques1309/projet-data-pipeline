"""
API Gold NYC-TLC — Lambda proxy monolithique.
 
Un seul handler, routage interne sur (méthode HTTP, chemin de ressource).
Chaque endpoint métier mappe une VIEW gold interrogée via Athena.
 
Colonnes alignées sur le schéma réel des 4 VIEWs (SHOW COLUMNS).
"""

import boto3
import time
import os
import json


DATABASE = os.environ["DATABASE"]                    
WORKGROUP = os.environ["WORKGROUP"]  
S3_OUTPUT = os.environ["ATHENA_OUTPUT_LOCATION"]     


RETRY_COUNT = 25       # nb d'itérations de polling avant timeout
POLL_INTERVAL = 0.5    # secondes entre deux vérifications de statut

athena = boto3.client("athena")

def _result_configuration():
    """
    ResultConfiguration Athena.
    Chiffrement des résultats assuré par la default encryption du bucket de sortie.
    Vérifier que le bucket ATHENA_OUTPUT_LOCATION a une default encryption activée.
    """
    return {"OutputLocation": S3_OUTPUT}


def run_query(sql, params=None):
    """
    Exécute une requête Athena paramétrée et renvoie les lignes en list[dict].
    params : valeurs pour les placeholders `?` positionnels (anti-injection natif).
    """
    kwargs = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": DATABASE},
        "ResultConfiguration": _result_configuration(),
        "WorkGroup": WORKGROUP,
    }
    if params:
        kwargs["ExecutionParameters"] = [str(p) for p in params]

    qid = athena.start_query_execution(**kwargs)["QueryExecutionId"]

    for _ in range(RETRY_COUNT):
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(POLL_INTERVAL)
    else:
        athena.stop_query_execution(QueryExecutionId=qid)
        raise TimeoutError(f"Athena query timeout after {RETRY_COUNT} polls")

    return _parse_results(qid)


def _parse_results(qid):
    """Réponse get_query_results (paginée) -> list[dict]. 1re ligne = en-tête."""
    rows = []
    header = None
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            values = [c.get("VarCharValue") for c in row["Data"]]
            if header is None:
                header = values
                continue
            rows.append(dict(zip(header, values)))
    return rows



def ep_revenue_by_zone_hour(qs):
    """
    Revenu par zone et par heure.
    Filtres optionnels : heure (0-23), service (yellow/green/fhvhv).
    toute valeur venant du client passe par ExecutionParameters (placeholders `?`),
    jamais par f-string -> pas d'injection SQL possible.
    """
    where, params = [], []
    if qs.get("heure") is not None:
        where.append("heure = ?")
        params.append(int(qs["heure"]))
    if qs.get("service"):
        where.append("service_type = ?")
        params.append(qs["service"])
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
        SELECT service_type, borough, zone, jour_semaine, heure,
               nb_courses, revenu_total, revenu_moyen, revenu_par_min,
               tip_pct, distance_moyenne
        FROM gold_revenue_by_zone_hour
        {clause}
        ORDER BY revenu_total DESC
        LIMIT 100
    """
    return run_query(sql, params)


def ep_demand_timeseries(qs):
    """
    Série temporelle de la demande.
    Filtres optionnels : service (yellow/green/fhvhv), date (YYYY-MM-DD).
    """
    where, params = [], []
    if qs.get("service"):
        where.append("service_type = ?")
        params.append(qs["service"])
    if qs.get("date"):
        where.append("date_course = ?")
        params.append(qs["date"])
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
        SELECT date_course, heure, jour_semaine, service_type,
               nb_courses, distance_moy, duree_moy_sec, vitesse_moy_mph
        FROM gold_demand_timeseries
        {clause}
        ORDER BY date_course, heure
        LIMIT 500
    """
    return run_query(sql, params)


def ep_service_comparison(qs):
    """
    Comparaison des services (yellow/green/fhvhv).
    Filtre optionnel : borough.
    """
    where, params = [], []
    if qs.get("borough"):
        where.append("borough = ?")
        params.append(qs["borough"])
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
        SELECT service_type, borough, nb_courses, part_marche_pct,
               panier_median, tip_pct, driver_share_pct
        FROM gold_service_comparison
        {clause}
        ORDER BY nb_courses DESC
    """
    return run_query(sql, params)


def ep_od_matrix(qs):
    """
    Matrice origine-destination (par borough).
    Filtres optionnels : origin (pu_borough), service.
    """
    where, params = [], []
    if qs.get("origin"):
        where.append("pu_borough = ?")
        params.append(qs["origin"])
    if qs.get("service"):
        where.append("service_type = ?")
        params.append(qs["service"])
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
        SELECT pu_borough, do_borough, service_type,
               nb_courses, distance_moy, revenu_moy
        FROM gold_od_matrix
        {clause}
        ORDER BY nb_courses DESC
        LIMIT 200
    """
    return run_query(sql, params)


ROUTES = {
    ("GET", "/revenue-by-zone-hour"): ep_revenue_by_zone_hour,
    ("GET", "/demand-timeseries"): ep_demand_timeseries,
    ("GET", "/service-comparison"): ep_service_comparison,
    ("GET", "/od-matrix"): ep_od_matrix,
}


def lambda_handler(event, context):
    method = event.get("httpMethod", "GET")
    path = (event.get("path") or "/").rstrip("/") or "/"
    qs = event.get("queryStringParameters") or {}

    handler = ROUTES.get((method, path))
    if handler is None:
        return _response(404, {"error": "Not found", "path": path, "method": method})

    try:
        data = handler(qs)
        return _response(200, {"count": len(data), "data": data})
    except (ValueError, KeyError) as e:
        return _response(400, {"error": "Bad request", "detail": str(e)})
    except TimeoutError as e:
        return _response(504, {"error": "Query timeout", "detail": str(e)})
    except Exception as e:
        print(f"ERROR: {e}")
        return _response(500, {"error": "Internal error"})


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",  # à restreindre en prod
        },
        "body": json.dumps(body, default=str),
    }