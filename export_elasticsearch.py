from lib.mappingJson import mappingJson
import lib.toolbox as toolbox
import time
import json
from elasticsearch import Elasticsearch

es = Elasticsearch(['localhost:9200'])
Mapping = mappingJson(es)

print("----- Préparation ElasticSearch pour la réception des données ------")
print("vidage index:", 'tp3', '>', 'stationsBixi', Mapping.emptyIndexType('tp3', 'stationsBixi'))
print("vidage index:", 'tp3', '>', 'stationsCommunauto', Mapping.emptyIndexType('tp3', 'stationsCommunauto'))
print("indexage mapping", 'tp3', '>', 'stationsBixi', Mapping.indexMapping('tp3', 'stationsBixi'))
print("indexage mapping", 'tp3', '>', 'stationsCommunauto', Mapping.indexMapping('tp3', 'stationsCommunauto'))
# Mapping.resetAllMapping()
# exit()

print("----- Lecture des TapIn et envoi à ElasticSearch ------")

def batchToElasticSearch(ligne, labels, batch, counts, es, index, docType, forceSave):
    ''' On veut envoyer les voyages à ES, mais toujours par lot de 10 000 '''
    step = 10000
    batchIndex = {"index": {}}

    ligneJson = {}
    j = 0
    for l in labels:
        parsedValue = Mapping.parseValue(index, docType, l, ligne[j])
        ligneJson[l] = parsedValue
        j += 1
    # print(ligneJson)
    batch.extend([batchIndex, ligneJson])

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
    i = -1
    for line in open(fileName):
        ligne = line.replace("\n", "").split(";")
        # name;lon;lat;coordsLatLon;capacity;region
        if i < 0:
            labels = ligne
            labels.append('date')
            i += 1
            continue
        ligne.append(date)
        toolbox.progressBar(i, 325)

        if i >= 10000000:
            break  # On veut que 500 tapIn mais ... on ne veut pas couper les tap in d'une carte !

        batchToElasticSearch(ligne, labels, batch, counts, es, index, docType, False)
        i += 1

    if batch:  # On regarde les tap In précédents pour des correspondances
        batchToElasticSearch(ligne, labels, batch, counts, es, index, docType, True)

    toolbox.hideProgressBar()
    print(counts["countAddedDocs"], "/", i+1, "transactions envoyées à ElasticSearch en", toolbox.tempsCalulString(tStart), "pour fileName")

exportFileToES("tp3", "stationsBixi", "input/dataStationsBixi 2014 reg2.csv", "2014-01-01")
exportFileToES("tp3", "stationsCommunauto", "input/dataStationsCommunauto 2010 reg2.csv", "2010-01-01")
