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
        "tp3": {
            "stationsBixi": {"_all": e._all, "properties": {
                "name": e.strNa,
                "lng": e.strNa,
                "lat": e.strNa,
                "coordsLngLat": e.location,
                "capacity": e.intEager,
                "date": e.date,
                "region": e.strNa
            }},
            "stationsCommunauto": {"_all": e._all, "properties": {
                "coordsLngLat": e.location,
                "lng": e.strNa,
                "lat": e.strNa,
                "NoStation": e.intEager,
                "Ville": e.strNa,
                "capacity": e.intEager,
                "date": e.date,
                "region": e.strNa
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
