library(SPARQLchunks)

namespaces <- "
PREFIX cube: <https://cube.link/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX nfi: <https://environment.ld.admin.ch/foen/nfi/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
"

lindasEndpoint <- "https://int.ld.admin.ch/query"

constructKeywordFilter <- function(filterTarget, keyword){
  if(keyword == ''){
    return('')
  }
  else{
    return(paste0("FILTER REGEX(", filterTarget, ", '", keyword, "', 'i') ."))
  }
}

constructLanguageFilter <- function(filterTarget, language){
  return(paste0("\tFILTER(LANG(", filterTarget, ") = '", language, "') ."))
}

getRegionNumber <- function(keyword="", language='en'){
  selectStatement <- "SELECT ?name ?number"
  fromStatement <- "FROM <https://lindas.admin.ch/foen/nfi>"
  whereStatement <- "{
    ?prop a rdf:Property .
    ?prop schema:name ?name .
    ?prop schema:identifier ?number .
    ?prop rdfs:subPropertyOf nfi:unitOfReference ."
  keywordFilter <- constructKeywordFilter('?name', keyword)
  languageFilter <- constructLanguageFilter('?name', language)
  endStatement <- "} order by asc(UCASE(?name))"
  
  query <- paste(namespaces ,selectStatement, fromStatement, whereStatement, 
                 keywordFilter, languageFilter, endStatement, sep="\n")
  
  return(query)
}

getTopicNumber <- function(keyword="", language="en"){
  selectStatement <- "SELECT ?name ?number"
  fromStatement <- "FROM <https://lindas.admin.ch/foen/nfi>"
  whereStatement <- "{
    ?prop a rdf:Property .
    ?prop schema:name ?name .
    ?prop schema:identifier ?number .
    ?prop rdfs:subPropertyOf nfi:targetValue ."
  keywordFilter <- constructKeywordFilter('?name', keyword)
  languageFilter <- constructLanguageFilter('?name', language)
  endStatement <- "} order by asc(UCASE(?name))"
  
  query <- paste(namespaces, selectStatement, fromStatement, whereStatement,
                 keywordFilter, languageFilter, endStatement, sep='\n')
  
  return(query)
}

getClassificationNumber <- function(keyword="", language="en"){
  selectStatement <- "SELECT ?name ?number"
  fromStatement <- "FROM <https://lindas.admin.ch/foen/nfi>"
  whereStatement <- "{
    ?prop a rdf:Property .
    ?prop schema:name ?name .
    ?prop schema:identifier ?number .
    ?prop rdfs:subPropertyOf nfi:classificationUnit ."
  keywordFilter <- constructKeywordFilter('?name', keyword)
  languageFilter <- constructLanguageFilter('?name', language)
  endStatement <- "} order by asc(UCASE(?name))"
  
  query <- paste(namespaces, selectStatement, fromStatement, whereStatement,
                 keywordFilter, languageFilter, endStatement, sep='\n')
  
  return(query)
}

getUnitOfEvaluationNumber <- function(keyword="", language="en"){
  selectStatement <- "SELECT ?name ?number"
  fromStatement <- "FROM <https://lindas.admin.ch/foen/nfi>"
  whereStatement <- "{
    ?prop a nfi:UnitOfEvaluation .
    ?prop schema:name ?name .
    ?prop schema:identifier ?number ."
  keywordFilter <- constructKeywordFilter('?name', keyword)
  languageFilter <- constructLanguageFilter('?name', language)
  endStatement <- "} order by asc(UCASE(?name))"
  
  query <- paste(namespaces, selectStatement, fromStatement, whereStatement,
                 keywordFilter, languageFilter, endStatement, sep='\n')
  
  return(query)
}

getTopicProperty <- function(topicNumber, relative=F){
  if(relative){
    relativeStatement = "FILTER regex(?topicIRIName, 'per HA of forest area$', 'i') ."
  }
  else{
    relativeStatement = "FILTER (!regex(?topicIRIName, 'per HA of forest area$', 'i')) ."
  }
  selectStatement <- "SELECT ?topicIRI"
  fromStatement <- "FROM <https://lindas.admin.ch/foen/nfi>"
  whereStatement <- paste0("{
    ?topicIRI rdfs:subPropertyOf nfi:targetValue .
    ?topicIRI schema:identifier ", topicNumber, " .
    ?topicIRI schema:name ?topicIRIName .
    FILTER(LANG(?topicIRIName) = 'en') .")
  endStatement <- "}"
  
  query <- paste(namespaces, selectStatement, fromStatement, whereStatement,
                 relativeStatement, endStatement)
  
  result <- sparql2df(endpoint = lindasEndpoint, query)$topicIRI[1]
  return(result)
}

getTopicSEProperty <- function(topicProperty){
  selectStatement <- "SELECT ?topicSEIRI"
  fromStatement <- "FROM <https://lindas.admin.ch/foen/nfi>"
  whereStatement <- paste0("{
    <", topicProperty, "> rdfs:seeAlso ?topicSEIRI . 
    } ")
  
  query <- paste(namespaces, selectStatement, fromStatement, whereStatement)
  
  result <- sparql2df(endpoint = lindasEndpoint, query)$topicSEIRI[1]
  return(result)
}

getClassificationProperty <- function(classificationNumber){
  selectStatement <- "SELECT ?classificationIRI"
  fromStatement <- "FROM <https://lindas.admin.ch/foen/nfi>"
  whereStatement <- paste0("{
    ?classificationIRI rdfs:subPropertyOf nfi:classificationUnit .
    ?classificationIRI schema:identifier ", classificationNumber, " .
  }")
  
  query <- paste(namespaces, selectStatement, fromStatement, whereStatement)
  
  result <- sparql2df(endpoint = lindasEndpoint, query)$classification[1]
  return(result)
}

getGeometry <- function(geometryIRI){
  geometryNamespace <- "PREFIX ogc: <http://www.opengis.net/ont/geosparql#>"
  selectStatement <- "SELECT ?poly"
  whereStatement <- paste0("{
    <", geometryIRI, "> ogc:asWKT ?poly
  }")
  query <- paste(geometryNamespace, selectStatement, whereStatement)

  endpointURL = "https://geo.ld.admin.ch/query"
  result <- sparql2list(endpoint = endpointURL, query = query)

  return(result$sparql$results$result$binding$literal[1][1])
}

getData <- function(topicNumber, classificationNumber, regionNumber, 
                    unitOfEvaluationNumber, language='en', geometry=F, relative=F){
  topicProperty <- getTopicProperty(topicNumber, relative)
  topicSEProperty <- getTopicSEProperty(topicProperty)
  classificationProperty <- getClassificationProperty(classificationNumber)
  
  # write the query
  selectStatement <- "SELECT DISTINCT ?inventoryName ?classificationName ?regionName ?unitOfEvaluationName ?topicValue ?topicSE ?geometryIRI"
  fromStatement <- "FROM <https://lindas.admin.ch/foen/nfi>"
  whereStatement <- paste0("{
    ?obs a cube:Observation ;
      nfi:inventory ?inventoryIRI ;
      <", topicProperty, "> ?topicValue ;
      <", topicSEProperty, "> ?topicSE ;
      <", classificationProperty, "> ?classificationIRI ;
      nfi:unitOfReference ?regionIRI ;
      nfi:unitOfEvaluation ?unitOfEvaluationIRI .")
  filterStatement <- paste0("
      ?regionIRI a ?regionTypeIRI .
      ?regionTypeIRI schema:identifier ", regionNumber, " .
      ?unitOfEvaluationIRI schema:identifier ", unitOfEvaluationNumber, " .
      ?regionIRI geo:hasGeometry ?geometryIRI .")
  nameLookupStatement <- paste0("
      ?inventoryIRI schema:name ?inventoryName . FILTER(lang(?inventoryName)='", language, "') .
      ?classificationIRI schema:name ?classificationName . FILTER(lang(?classificationName)='", language, "') .
      ?regionIRI schema:name ?regionName . FILTER(lang(?regionName)='", language, "') .
      ?unitOfEvaluationIRI schema:name ?unitOfEvaluationName . FILTER(lang(?unitOfEvaluationName)='", language, "') .")
  
  query <- paste(namespaces, selectStatement, fromStatement, whereStatement, filterStatement, nameLookupStatement, "}")
  
  data <- sparql2df(endpoint = lindasEndpoint, query)
  
  if(geometry){
    geometryDict <- c()
    for(region in unique(data$geometryIRI)){
      geometryDict[region] = getGeometry(region)
    }
    data$Geometry <- ""
    for (entry in data$geometryIRI){
      data$Geometry <- geometryDict[entry]
    }
  }
  
  return(data)
}