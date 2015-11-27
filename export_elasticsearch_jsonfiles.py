from lib.mappingJson import mappingJson
import lib.toolbox as toolbox
import time
import json
from elasticsearch import Elasticsearch

es = Elasticsearch(['localhost:9200'])
Mapping = mappingJson(es)

print("----- Préparation ElasticSearch pour la réception des données ------")
print("vidage index:", 'defivelomtl', '>', 'mrvtripcleanliv03', Mapping.emptyIndexType('defivelomtl', 'mrvtripcleanliv03'))
print("indexage mapping", 'defivelomtl', '>', 'mrvtripcleanliv03', Mapping.indexMapping('defivelomtl', 'mrvtripcleanliv03'))
print("vidage index:", 'defivelomtl', '>', 'mrvtripsnapliv03', Mapping.emptyIndexType('defivelomtl', 'mrvtripsnapliv03'))
print("indexage mapping", 'defivelomtl', '>', 'mrvtripsnapliv03', Mapping.indexMapping('defivelomtl', 'mrvtripsnapliv03'))
# Mapping.resetAllMapping()
# exit()

print("----- Lecture des TapIn et envoi à ElasticSearch ------")


def batchToElasticSearch(feature, batch, counts, es, index, docType, forceSave):
    ''' On veut envoyer les voyages à ES, mais toujours par lot de 10 000 '''
    step = 10000
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
    skip = 5
    i = 0
    for line in open(fileName, encoding="utf8"):
        ligne = line.replace("\n", "")

        if skip:
            skip -= 1
            continue
        if ligne == "]":
            break  # on est à la fin du fichier
        if ligne[-2:] == "},":
            ligne = ligne[:-1]
        feature = json.loads(ligne)
        if date:
            feature.date = date
        else:
            feature["properties"]["date"] = feature["properties"]["start"]

        toolbox.progressBar(i, 150000)

        if i >= 10000000:
            break  # On veut que 500 tapIn mais ... on ne veut pas couper les tap in d'une carte !

        batchToElasticSearch(feature, batch, counts, es, index, docType, False)
        i += 1

    if batch:  # On regarde les tap In précédents pour des correspondances
        batchToElasticSearch(feature, batch, counts, es, index, docType, True)

    toolbox.hideProgressBar()
    print(counts["countAddedDocs"], "/", i+1, "transactions envoyées à ElasticSearch en", toolbox.tempsCalulString(tStart), "pour", fileName)

print("export defivelomtl > mrvtripcleanliv03")
exportFileToES("defivelomtl", "mrvtripcleanliv03", "input/mrvtripcleanliv03.json", False)

print("export defivelomtl > mrvtripsnapliv03")
exportFileToES("defivelomtl", "mrvtripsnapliv03", "input/mrvtripsnapliv03.json", False)
