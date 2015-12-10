from lib.mappingJson import mappingJson
import lib.toolbox as toolbox
import time
import os
import json
from elasticsearch import Elasticsearch

es = Elasticsearch(['localhost:9200'])
Mapping = mappingJson(es)

print("----- Préparation ElasticSearch pour la réception des données ------")
# print("vidage index:", 'defivelomtl', '>', 'bixi_stations', Mapping.emptyIndexType('defivelomtl', 'bixi_stations'))
# print("indexage mapping", 'defivelomtl', '>', 'bixi_stations', Mapping.indexMapping('defivelomtl', 'bixi_stations'))
# print("vidage index:", 'defivelomtl', '>', 'bixi_OD', Mapping.emptyIndexType('defivelomtl', 'bixi_OD'))
# print("indexage mapping", 'defivelomtl', '>', 'bixi_OD', Mapping.indexMapping('defivelomtl', 'bixi_OD'))
# print("vidage index:", 'defivelomtl', '>', 'arceaux_a_velos', Mapping.emptyIndexType('defivelomtl', 'arceaux_a_velos'))
# print("indexage mapping", 'defivelomtl', '>', 'arceaux_a_velos', Mapping.indexMapping('defivelomtl', 'arceaux_a_velos'))
# print("vidage index:", 'defivelomtl', '>', 'precipitations_jours', Mapping.emptyIndexType('defivelomtl', 'precipitations_jours'))
# print("indexage mapping", 'defivelomtl', '>', 'precipitations_jours', Mapping.indexMapping('defivelomtl', 'precipitations_jours'))
print("vidage index:", 'defivelomtl', '>', 'polluants', Mapping.emptyIndexType('defivelomtl', 'polluants'))
print("indexage mapping", 'defivelomtl', '>', 'polluants', Mapping.indexMapping('defivelomtl', 'polluants'))
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

    for line in open(fileName, encoding="utf8"):
        ligne = line.replace("\n", "").split(";")
        # name;lon;lat;coordsLatLon;capacity;region
        if i < 0:
            labels = ligne
            if date:
                labels.append('date')
            else:
                if docType == "bixi_OD":
                    labels.append('OD_stationsIds')
                    labels.append('date')
                    labels.append('hour')
                    labels.append('tempsTrajet')
                    labels.append('Start Coords LngLat')
                    labels.append('End Coords LngLat')
            i += 1
            continue
        if date:
            ligne.append(date)
        else:
            if docType == "bixi_OD":
                # 0:StartDate, 1:StartStationNumber, 2:StartStation, 3:EndDate, 4:EndStationNumber, 5:EndStation, 6:AccountType, 7:MemberGender, 8:TotalDuration, 9:MemberLanguage
                ligne.append(ligne[1]+"-"+ligne[4])
                ligne.append(ligne[0])
                ligne.append(ligne[0][-5:-3])
                try:
                    tempsTrajet = ligne[8].split("m")
                    tempsTrajet = tempsTrajet[0].split("h")
                    if len(tempsTrajet) == 2:
                        tempsTrajet = int(tempsTrajet[0])*60 + int(tempsTrajet[1])
                    else:
                        tempsTrajet = int(tempsTrajet[0])
                except Exception:
                    print("buug", ligne[8])
                ligne.append(tempsTrajet)
                try:
                    ligne.append(bixi_stops[ligne[1]])
                except Exception:
                    print("OD", ligne[1], "->", ligne[4], "éliminée, station", ligne[1], "inexistante", "ligne:", ligne)
                    continue
                try:
                    ligne.append(bixi_stops[ligne[4]])
                except Exception:
                    print("OD", ligne[1], "->", ligne[4], "éliminée, station", ligne[4], "inexistante", "ligne:", ligne)
                    continue
        toolbox.progressBar(i, 700000)

        if i >= 2000000:
            break  # On veut que 500 tapIn mais ... on ne veut pas couper les tap in d'une carte !

        batchToElasticSearch(ligne, labels, batch, counts, es, index, docType, False)
        i += 1

    if batch:  # On regarde les tap In précédents pour des correspondances
        batchToElasticSearch(ligne, labels, batch, counts, es, index, docType, True)

    toolbox.hideProgressBar()
    print(counts["countAddedDocs"], "/", i+1, "transactions envoyées à ElasticSearch en", toolbox.tempsCalulString(tStart), "pour", fileName)

bixi_stops = {}
i = -1
for line in open("input/BIXI_Stations_20151126.csv", encoding="utf8"):
    if i < 0:
        i += 1
        # print("skip !")
        continue
    ligne = line.replace("\n", "").split(";")
    # terminalName;id;name;lat;long;coordsLngLat;installed;locked;temporary;public;nbBikes;nbEmptyDocks
    bixi_stops[ligne[0]] = ligne[5]

# print("export defivelomtl > bixi_stations")
# exportFileToES("defivelomtl", "bixi_stations", "input/BIXI_Stations_20151126.csv", "2015-11-26")

# for file in os.listdir("input/bixi_OD"):
#     print("export defivelomtl > bixi_OD > ", file)
#     exportFileToES("defivelomtl", "bixi_OD", "input/bixi_OD/"+file, False)

# print("export defivelomtl > arceaux_a_velos")
# exportFileToES("defivelomtl", "arceaux_a_velos", "input/arceaux_a_velos.csv", "2013-10-17")

# print("export defivelomtl > precipitations_jours")
# exportFileToES("defivelomtl", "precipitations_jours", "input/precipitations_jours.csv", False)

print("export defivelomtl > polluants")
exportFileToES("defivelomtl", "polluants", "input/polluants.csv", False)
