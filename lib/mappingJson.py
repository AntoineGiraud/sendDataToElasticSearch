import json


class mappingJson:

    """ mapping des index/type d'ElasticSearch """

    def __init__(self, ElasticSearch):
        self.es = ElasticSearch

    class e:
        str = {"type": "string"}
        strNa = {"type": "string", "index": "not_analyzed", "doc_values": True}
        float = {"type": "float"}
        _all = {"enabled": False}
        location = {"type": "geo_point", "doc_values": True}
        int = {"type": "integer", "doc_values": True}
        intEager = {"type": "integer", "doc_values": True, "fielddata": {"loading": "eager"}}
        timestamp = {"type": "date", "doc_values": True, "format": "YYYY-MM-dd HH:mm"}
        timestampWithSeconds = {"type": "date", "doc_values": True, "format": "YYYY-MM-dd HH:mm:ss"}
        timestampFr = {"type": "date", "doc_values": True, "format": "dd/MM/YYYY HH:mm"}
        date = {"type": "date", "doc_values": True, "format": "YYYY-MM-dd"}
        hourMin = {"type": "date", "doc_values": True, "format": "HH:mm"}
        station = {
            "id": strNa,
            "stop_id": strNa,
            "SRIDU": strNa,
            "mode": strNa,
            "lat": strNa,
            "location": location,
            "lon": strNa,
            "name": strNa,
            "name_lat_lon": strNa
        }
        stationShort = {
            "stop_id": strNa,
            "SRIDU": strNa,
            "location": location
        }
        stationP = {"properties": station}
        stationShortP = {"properties": stationShort}
        # curl -XPOST localhost:9200/stations/stations/_bulk --data-binary @Stations_bulk.json

    mappings = {
        "defivelomtl": {
            "bixi_stations": {"_all": e._all, "properties": {
                "id": e.int,
                "name": e.strNa,
                "terminalName": e.int,
                "lat": e.strNa,
                "long": e.strNa,
                "coordsLngLat": e.location,
                "installed": e.int,
                "locked": e.int,
                "temporary": e.int,
                "public": e.int,
                "nbBikes": e.int,
                "nbEmptyDocks": e.int,
                "coordsLngLat": e.location,
                "date": e.date
            }},
            "bixi_OD": {"_all": e._all, "properties": {
                "date": e.timestampFr,
                "Start date": e.timestampFr,
                "Start station number": e.int,
                "Start station": e.strNa,
                "Start Coords LngLat": e.location,
                "End date": e.timestampFr,
                "End station number": e.int,
                "End station": e.strNa,
                "End Coords LngLat": e.location,
                "Account type": e.strNa,
                "Member's gender": e.strNa,
                "Total duration": e.strNa,
                "Member's language": e.strNa,
                "min": e.int
            }},
            "arceaux_a_velos": {"_all": e._all, "properties": {
                "INV_ID": e.strNa,
                "INV_NO": e.strNa,
                "ANC_NUM": e.strNa,
                "INV_CATL_NO": e.strNa,
                "CATL_MODELE": e.strNa,
                "MARQ": e.strNa,
                "DATE_INSPECTION": e.strNa,
                "CE_NO": e.strNa,
                "ELEMENT": e.strNa,
                "CATEGORIE": e.strNa,
                "COULEUR": e.strNa,
                "MATERIAU": e.strNa,
                "CONDITION": e.strNa,
                "INTERVENTION": e.strNa,
                "EMPL_X": e.strNa,
                "EMPL_Y": e.strNa,
                "EMPL_Z": e.strNa,
                "TERRITOIRE": e.strNa,
                "STATUT": e.strNa,
                "BASE": e.strNa,
                "ANCRAGE": e.strNa,
                "PARC": e.strNa,
                "AIRE": e.strNa,
                "EMPL_ID": e.strNa,
                "ORDRE_AFFICHAGE": e.strNa,
                "LONG": e.strNa,
                "LAT": e.strNa,
                "coordsLngLat": e.location,
                "date": e.date
            }},
            "mrvtripsnapliv03": {"_all": e._all, "properties": {
                "type": e.strNa,
                "properties": {
                    "type": "object",
                    "properties": {
                      "id": e.int,
                      "purpose": e.strNa,
                      "notes": e.strNa,
                      "date": e.timestampWithSeconds,
                      "start": e.timestampWithSeconds,
                      "stop": e.timestampWithSeconds,
                      "length": e.int,
                      "id_origine": e.int
                    }
                },
                "geometry": {
                    "type": "object",
                    "properties": {
                      "type": e.strNa,
                      "coordinates": e.location
                    }
                }
            }},
            "mrvtripcleanliv03": {"_all": e._all, "properties": {
                "type": e.strNa,
                "properties": {
                    "type": "object",
                    "properties": {
                        "id": e.int,
                        "purpose": e.strNa,
                        "notes": e.strNa,
                        "date": e.timestampWithSeconds,
                        "start": e.timestampWithSeconds,
                        "stop": e.timestampWithSeconds,
                        "length": e.int,
                        "liste_segments": e.strNa,
                        "id_origine": e.int
                    }
                },
                "geometry": {
                    "type": "object",
                    "properties": {
                        "type": e.strNa,
                        "coordinates": e.location
                    }
                }
            }}
        }
    }

    def getMapping(self, index, type):
        return {type: self.mappings[index][type]}

    def parseValue(self, index, type, field, value):
        fieldCarac = self.mappings[index][type]["properties"][field]
        if fieldCarac == self.e.int or fieldCarac == self.e.intEager:
            return int(value)
        elif fieldCarac == self.e.float:
            return float(value)
        elif fieldCarac == self.e.location:
            return json.loads(value)
        else:
            return value

    def emptyIndexType(self, index, type):
        ''' Attention, les documents de l'index concerné vont être perdus, son mapping aussi '''
        try:
            return self.es.delete(index=index, doc_type=type)
        except Exception:
            return True

    def getEsMapping(self, index, type):
        return self.es.get(index=index, doc_type='_mapping', id=type)

    def indexMapping(self, index, type):
        ''' Attention, les documents de l'index concerné vont être perdus '''
        try:
            self.es.delete(index=index, doc_type='_mapping', id=type)
        except Exception:
            pass
        return self.es.index(index=index, doc_type='_mapping', id=type, body=self.getMapping(index, type))

    def resetAllMapping(self):
        ''' Attention, les documents des index concernés vont être perdus '''
        for index, types in self.mappings.items():
            for type, doc in types.items():
                print('reset mapping:', index, '>', type, self.indexMapping(index, type))
