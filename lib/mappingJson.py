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
            "precipitations_jours": {"_all": e._all, "properties": {
                "date": e.date,
                "Précipitations": e.float,
                "Température minimale": e.float,
                "Température maximale": e.float,
                "Température moyenne": e.float
            }},
            "bixi_stations": {"_all": e._all, "properties": {
                "id": e.int,
                "name": e.strNa,
                "terminalName": e.int,
                "lat": e.strNa,
                "long": e.strNa,
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
                "date": e.timestamp,
                "hour": e.int,
                "OD_stationsIds": e.strNa,
                "Start date": e.timestamp,
                "Start station number": e.int,
                "Start station": e.strNa,
                "Start Coords LngLat": e.location,
                "End date": e.timestamp,
                "End station number": e.int,
                "End station": e.strNa,
                "End Coords LngLat": e.location,
                "Account type": e.strNa,
                "Member's gender": e.strNa,
                "Total duration": e.strNa,
                "Member's language": e.strNa,
                "tempsTrajet": e.int
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
            "polluants": {"_all": e._all, "properties": {
                "NO_POSTE": e.strNa,
                "date": e.timestamp,
                "CO": e.float,
                "H2S": e.float,
                "NO": e.float,
                "NO2": e.float,
                "PM2_5": e.float,
                "PM2_5F": e.float,
                "PM10": e.float,
                "O3": e.float,
                "SO2": e.float,
                "lat": e.strNa,
                "lng": e.strNa,
                "coordsLngLat": e.location
            }},
            "trip5000MonReseauVelo": {"_all": e._all, "properties": {
                "type": e.strNa,
                "date": e.timestampWithSeconds,
                "hour": e.int,
                "properties": {
                    "type": "object",
                    "properties": {
                        "id": e.int,
                        "id_origine": e.int,
                        "n_coord": e.int,
                        "purpose": e.strNa,
                        # "notes": e.strNa,
                        "start": e.timestampWithSeconds,
                        "stop": e.timestampWithSeconds,
                        "length": e.int,
                        "liste_segments_jsonb": {
                            "type": "object",
                            "properties": {
                                "id": e.int,
                                "source": e.strNa
                            }
                        },
                        "speeds": e.int,
                        "accelerations": e.float,
                        "n_points": e.int,
                        "n_seconds": e.int,
                        "mean_speed": e.float,
                        "startPoint": e.location,
                        "endPoint": e.location,
                        "wiggliness": e.float
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
            "charges_bixi": {"_all": e._all, "properties": {
                "type": e.strNa,
                "date": e.date,
                "geometry": {
                    "type": "object",
                    "properties": {
                        "type": e.strNa,
                        "coordinates": e.location
                    }
                },
                "properties": {
                    "type": "object",
                    "properties": {
                        "charge": e.int
                    }
                }
            }},
            "chemins_plus_courts_od_bixi": {"_all": e._all, "properties": {
                "type": e.strNa,
                "date": e.date,
                "geometry": {
                    "type": "object",
                    "properties": {
                        "type": e.strNa,
                        "coordinates": e.location
                    }
                },
                "properties": {
                    "type": "object",
                    "properties": {
                        "nb_transactions": e.int
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
            return int(value or 0)
        elif fieldCarac == self.e.float:
            return float(value or 0)
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
