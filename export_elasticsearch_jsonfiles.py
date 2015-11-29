from lib.mappingJson import mappingJson
import lib.toolbox as toolbox
import time
import json
from elasticsearch import Elasticsearch

es = Elasticsearch(['localhost:9200'])
Mapping = mappingJson(es)

print("----- Préparation ElasticSearch pour la réception des données ------")
print("vidage index:", 'defivelomtl', '>', 'trip5000MonReseauVelo', Mapping.emptyIndexType('defivelomtl', 'trip5000MonReseauVelo'))
print("indexage mapping", 'defivelomtl', '>', 'trip5000MonReseauVelo', Mapping.indexMapping('defivelomtl', 'trip5000MonReseauVelo'))
print("vidage index:", 'defivelomtl', '>', 'charges_bixi', Mapping.emptyIndexType('defivelomtl', 'charges_bixi'))
print("indexage mapping", 'defivelomtl', '>', 'charges_bixi', Mapping.indexMapping('defivelomtl', 'charges_bixi'))
# print("vidage index:", 'defivelomtl', '>', 'mrvtripcleanliv03', Mapping.emptyIndexType('defivelomtl', 'mrvtripcleanliv03'))
# print("indexage mapping", 'defivelomtl', '>', 'mrvtripcleanliv03', Mapping.indexMapping('defivelomtl', 'mrvtripcleanliv03'))
# print("vidage index:", 'defivelomtl', '>', 'mrvtripsnapliv03', Mapping.emptyIndexType('defivelomtl', 'mrvtripsnapliv03'))
# print("indexage mapping", 'defivelomtl', '>', 'mrvtripsnapliv03', Mapping.indexMapping('defivelomtl', 'mrvtripsnapliv03'))
# Mapping.resetAllMapping()
# exit()

print("----- Lecture des TapIn et envoi à ElasticSearch ------")


def batchToElasticSearch(feature, batch, counts, es, index, docType, forceSave):
    ''' On veut envoyer les voyages à ES, mais toujours par lot de 10 000 '''
    step = 500
    batchIndex = {"index": {}}

    batch.extend([batchIndex, feature])

    if counts["i"] % step == 0 and counts["i"] > 0:
        res = es.bulk(index=index, doc_type=docType, body=batch)
        counts["countAddedDocs"] += len(res['items'])
        del batch[:]
    if forceSave and len(batch) > 0:
        res = es.bulk(index=index, doc_type=docType, body=batch)
        counts["countAddedDocs"] += len(res['items'])
        del batch[:]
        return
    counts["i"] += 1


def exportFileToES(index, docType, fileName, date):
    tStart = time.clock()
    batch = []
    labels = []

    counts = {
        "i": 0,
        "countAddedDocs": 0,
    }

    countAddedDocs = 0
    i = 0

    data = json.loads(open(fileName).readline())
    # ['type', 'geometry', 'properties']
    # geometry: {'type', 'coordinates'}
    # properties: ['n_coord', 'length', 'purpose', 'start', 'id_origine',
    # 'id', 'liste_segments_jsonb', 'stop']

    for feature in data['features']:
        if date:
            feature["date"] = date
        else:
            feature["date"] = feature["properties"]["start"]
            try:
                feature["hour"] = int(feature["date"][-8:-6])
            except Exception:
                print("buuug", feature["properties"])

            english2French = {
                'Commute': 'Domicile-travail',
                'Errand': 'Courses',
                'Exercise': 'Sport',
                'Leisure': 'Loisirs',
                'Other': 'Autre',
                'Autres': 'Autre',
                'School': 'École',
                'Shopping': 'Magasinage',
                'Work-Related': 'Travail',
                'Work-related': 'Travail',
                'Other': 'Autre',
                'other': 'Autre'
            }
            feature['properties']['purpose'] = english2French.get(feature['properties']['purpose'], feature['properties']['purpose'])

            if len(feature["geometry"]["coordinates"]) >= 2:
                feature['properties']["startPoint"] = feature["geometry"]["coordinates"][0]
                feature['properties']["endPoint"] = feature["geometry"]["coordinates"][-1]
            else:
                print("Oh, baa ya qu'un point ou pas du tout")

            # print(feature["date"][-8:-3])
            # break

        toolbox.progressBar(i, 15800)

        if i >= 100000:
            # On veut que 500 tapIn mais ... on ne veut pas couper les tap in
            # d'une carte !
            break

        batchToElasticSearch(feature, batch, counts, es, index, docType, False)
        i += 1

    if batch:  # On regarde les tap In précédents pour des correspondances
        batchToElasticSearch(feature, batch, counts, es, index, docType, True)

    toolbox.hideProgressBar()
    print(counts["countAddedDocs"], "/", i + 1, "transactions envoyées à ElasticSearch en",
          toolbox.tempsCalulString(tStart), "pour", fileName)

print("export defivelomtl > trip5000-enriched.json")
exportFileToES("defivelomtl", "trip5000MonReseauVelo", "input/trip5000-enriched.json", False)

print("export defivelomtl > charges_bixi")
exportFileToES("defivelomtl", "charges_bixi", "input/charges_bixi.json", "2015-11-28")
