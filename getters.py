import pandas as pd
import geopandas as gpd
import shapely.wkt
from SPARQLWrapper import SPARQLWrapper, CSV, JSON
from io import StringIO

global namespaces
namespaces = (
    "PREFIX cube: <https://cube.link/>\n"
    "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
    "PREFIX nfi: <https://environment.ld.admin.ch/foen/nfi/>\n"
    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
    "PREFIX schema: <http://schema.org/>\n\n"
)


# noinspection SqlNoDataSourceInspection
class LDLFI:
    _endpointURL = "https://int.ld.admin.ch/query"

    @staticmethod
    def getResults(query, returnFormat=CSV):
        endpoint_url = LDLFI._endpointURL
        sparql = SPARQLWrapper(endpoint_url, returnFormat=returnFormat)
        sparql.setQuery(query)
        sparql.setMethod("POST")
        result = sparql.queryAndConvert()
        if returnFormat == CSV:
            _csv = StringIO(result.decode('utf-8'))
            return pd.read_csv(_csv)
        elif returnFormat == JSON:
            return result['results']['bindings'][0]

    @classmethod
    def getRegionNumbers(cls, keyword=None, language='en'):
        keyword_filter = cls._constructKeywordFilter('?name', keyword)
        language_filter = cls._constructLanguageFilter('?name', language)

        query = namespaces + ("""
            SELECT ?name ?number
            FROM <https://lindas.admin.ch/foen/nfi>
            {{
              ?prop a rdf:Property .
              ?prop schema:name ?name .
              ?prop schema:identifier ?number .
              ?prop rdfs:subPropertyOf nfi:unitOfReference .
              {0} {1}
            }} order by asc(UCASE(str(?name)))""").format(keyword_filter, language_filter)

        print(cls.getResults(query))

    @classmethod
    def getClassificationNumbers(cls, keyword=None, language='en'):
        keyword_filter = cls._constructKeywordFilter('?name', keyword)
        language_filter = cls._constructLanguageFilter('?name', language)

        query = namespaces + ("""
            select ?name ?number
            from <https://lindas.admin.ch/foen/nfi>
            {{
              ?prop a rdf:Property .
              ?prop schema:name ?name .
              ?prop schema:identifier ?number .
              ?prop rdfs:subPropertyOf nfi:classificationUnit .
              {0} {1}
            }} order by asc(UCASE(str(?name)))""").format(keyword_filter, language_filter)

        print(cls.getResults(query))

    @classmethod
    def getTopicNumbers(cls, keyword=None, language='en'):
        keyword_filter = cls._constructKeywordFilter('?name', keyword)
        language_filter = cls._constructLanguageFilter('?name', language)

        query = namespaces + ("""
            SELECT ?name ?number
            FROM <https://lindas.admin.ch/foen/nfi>
            {{
              ?prop a rdf:Property .
              ?prop schema:name ?name .
              ?prop schema:identifier ?number .
              ?prop rdfs:subPropertyOf nfi:targetValue .
              {0} {1}
            }} order by asc(UCASE(str(?name)))""").format(keyword_filter, language_filter)

        print(cls.getResults(query))

    @classmethod
    def getUnitOfEvaluationNumbers(cls, keyword=None, language='en'):
        keyword_filter = cls._constructKeywordFilter('?name', keyword)
        language_filter = cls._constructLanguageFilter('?name', language)

        query = namespaces + ("""
            SELECT ?name ?number
            FROM <https://lindas.admin.ch/foen/nfi>
            {{
              ?prop a nfi:UnitOfEvaluation .
              ?prop schema:name ?name .
              ?prop schema:identifier ?number .
              {0} {1}
            }} order by asc(UCASE(str(?name)))""").format(keyword_filter, language_filter)

        print(cls.getResults(query))

    @classmethod
    def getData(cls, topicNumber, classificationNumber, regionNumber, unitOfEvaluationNumber, language='en', geometry=False):
        relative = topicNumber.endswith('r')
        topicProperty = cls._getTopicProperty(topicNumber, relative)
        topicSEProperty = cls._getTopicSEProperty(topicProperty)
        classificationProperty = cls._getClassificationProperty(classificationNumber)

        # Get the base data from lindas
        query = namespaces + ("""
            SELECT DISTINCT ?inventoryName ?classificationName ?regionName ?unitOfEvaluationName ?topicValue ?topicSE ?geometryIRI
            FROM <https://lindas.admin.ch/foen/nfi>
            {{ 
              # Base Lookup
              ?obs a cube:Observation ;
                nfi:inventory ?inventoryIRI ;
                <{0}> ?topicValue ;
                <{1}> ?topicSE ;
                <{2}> ?classificationIRI ;
                nfi:unitOfReference ?regionIRI ;
                nfi:unitOfEvaluation ?unitOfEvaluationIRI .
              
              # Filters
              ?regionIRI a ?regionTypeIRI .
              ?regionTypeIRI schema:identifier {3} .
              ?unitOfEvaluationIRI schema:identifier {4} .
              ?regionIRI geo:hasGeometry ?geometryIRI .
              
              # Name Lookup
              ?inventoryIRI schema:name ?inventoryName . FILTER(lang(?inventoryName)='{5}') .
              ?classificationIRI schema:name ?classificationName . FILTER(lang(?classificationName)='{5}') .
              ?regionIRI schema:name ?regionName . FILTER(lang(?regionName)='{5}') .
              ?unitOfEvaluationIRI schema:name ?unitOfEvaluationName . FILTER(lang(?unitOfEvaluationName)='{5}') .
            }}""").format(topicProperty, topicSEProperty, classificationProperty, regionNumber, unitOfEvaluationNumber,
                          language)

        data = cls.getResults(query)

        # get the geometries from geo.admin.ch
        # for performance issue, it's advised to first define a dictionary with all possible geometries
        if geometry:
            polydict = {}
            for geo in data.geometryIRI.unique():
                polydict[geo] = cls._getGeometry(geo)
            data['geometry'] = [polydict[geo] for geo in data.geometryIRI]

        # drop the now unneeded column
        data = data.drop(['geometryIRI'], axis=1)

        # finally, convert to geopandas
        if geometry:
            data = gpd.GeoDataFrame(data, geometry="geometry")
        return data

    @staticmethod
    def _getGeometry(geometryIRI):
        query = """
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX ogc: <http://www.opengis.net/ont/geosparql#>
        SELECT ?poly
        {{
          <{}> ogc:asWKT ?poly .
        }}
        """.format(geometryIRI)
        endpoint_url = "https://geo.ld.admin.ch/query"
        sparql = SPARQLWrapper(endpoint_url, returnFormat=JSON)
        sparql.setQuery(query)
        # sparql.setMethod("POST")
        result = sparql.queryAndConvert()
        geometryString = result['results']['bindings'][0]['poly']['value']
        return shapely.wkt.loads(geometryString)

    # @staticmethod
    # def _constructTopicFilter(topicNumber, relative=False):
    #     topicFilter = """
    #         # look up the topic IRI
    #         ?topicIRI rdfs:subPropertyOf nfi:targetValue .
    #         ?topicIRI schema:identifier {} .
    #         ?topicIRI schema:name ?topicIRIName .
    #         FILTER(LANG(?topicIRIName) = 'en') .
    #
    #         # find the corresponding standard error
    #         ?topicIRI rdfs:seeAlso ?topicSEIRI .
    #         ?obs ?topicSEIRI ?zielgrSE .
    #
    #     """.format(topicNumber)
    #     if relative:
    #         topicFilter += "FILTER regex(?topicIRIName, 'per HA of forest area$', 'i') ."
    #     else:
    #         topicFilter += "FILTER (!regex(?topicIRIName, 'per HA of forest area$', 'i')) ."
    #
    #     return topicFilter

    @classmethod
    def _getTopicProperty(cls, topicNumber, relative=False):
        if relative:
            relativeClause = "FILTER regex(?topicIRIName, 'per HA of forest area$', 'i') ."
        else:
            relativeClause = "FILTER (!regex(?topicIRIName, 'per HA of forest area$', 'i')) ."

        query = namespaces + """
        SELECT ?topicIRI
        FROM <https://lindas.admin.ch/foen/nfi>
        {{
            ?topicIRI rdfs:subPropertyOf nfi:targetValue .
            ?topicIRI schema:identifier {} .
            ?topicIRI schema:name ?topicIRIName .

            FILTER(LANG(?topicIRIName) = 'en') .
            {}
        }}
        """.format(topicNumber, relativeClause)

        result = cls.getResults(query, returnFormat=JSON)
        return result['topicIRI']['value']

    @classmethod
    def _getTopicSEProperty(cls, topicProperty):
        query = namespaces + """
        SELECT ?topicSEIRI
        FROM <https://lindas.admin.ch/foen/nfi>
        {{
            <{}> rdfs:seeAlso ?topicSEIRI .
        }}
        """.format(topicProperty)

        result = cls.getResults(query, returnFormat=JSON)
        return result['topicSEIRI']['value']

    @classmethod
    def _getClassificationProperty(cls, classificationNumber):
        query = namespaces + """
        SELECT ?classificationIRI 
        FROM <https://lindas.admin.ch/foen/nfi>
        {{
            ?classificationIRI rdfs:subPropertyOf nfi:classificationUnit .
            ?classificationIRI schema:identifier {} .
        }}
        """.format(classificationNumber)

        result = cls.getResults(query, returnFormat=JSON)
        return result['classificationIRI']['value']

    @staticmethod
    def _constructLanguageFilter(filter_target, language):
        return "FILTER(LANG({}) = '{}') .".format(filter_target, language)

    @staticmethod
    def _constructKeywordFilter(filter_target, keyword):
        if keyword is None:
            keyword_filter = ""
        else:
            keyword_filter = "FILTER REGEX({}, '{}', 'i') .".format(filter_target, keyword)

        return keyword_filter

    def constructQuery(language="'en'"):
        query = """PREFIX schema: <http://schema.org/>
        PREFIX cube: <https://cube.link/>
        prefix nfi: <https://environment.ld.admin.ch/foen/nfi/>

        select ?inventory_Name ?ausseinh_Name ?befnr1_Name ?uoe_Name ?zielgr
        from <https://lindas.admin.ch/foen/nfi>
        {{
          # primary lookup
          ?obs a cube:Observation .
          ?obs nfi:inventory ?inventory .
          ?inventory schema:name ?inventory_Name .
          ?obs nfi:unitOfEvaluation ?uoe .
          ?uoe schema:name ?uoe_Name .
          ?obs nfi:grid <https://environment.ld.admin.ch/foen/nfi/Grid/N4> .

          # lookup ausseinh
          ?obs {0} ?ausseinh .
          ?ausseinh schema:name ?ausseinh_Name .

          # lookup befnr1
          ?obs {1} ?befnr1 .
          ?befnr1 schema:name ?befnr1_Name .

          # lookup zielgr
          ?obs nfi:totalStemNumber ?zielgr .
          FILTEr(lang(?inventory_Name)={2}) .
          FILTEr(lang(?uoe_Name)={2}) .
          FILTer(lang(?ausseinh_Name)={2}) .
          FILTER(lang(?befnr1_Name)={2}) .
        }}""".format("nfi:prodreg", "nfi:lbhndh", language)
        return query
