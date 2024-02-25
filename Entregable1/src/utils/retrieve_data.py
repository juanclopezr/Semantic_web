from requests import get as get_request

GENERAL_FIELDS = [
    'paperId',
    'corpusId', # Su tipado es inconsistente, podría traer errores.
    'url',
    'title',
    'venue',
    'publicationVenue',
    'year',
    'authors',
    'externalIds',
    'abstract',
    'referenceCount',
    'citationCount',
    'influentialCitationCount',
    'isOpenAccess',
    'openAccessPdf',
    'fieldsOfStudy',
    's2FieldsOfStudy',
    'publicationTypes',
    'publicationDate',
    'journal',
]

AUTHORS_FIELDS = [
    'authorId',
    'externalIds', # No siempre esta presente, podría traer errores.
    'url',
    'name',
    'aliases',
    'affiliations',
    'paperCount',
    'citationCount'
]

fields = GENERAL_FIELDS + ['tldr'] + [f'authors.{i}' for i in AUTHORS_FIELDS]
fields += [f'{j}.{i}' for j in ['references', 'citations']
           for i in GENERAL_FIELDS]

QUERY_FIELDS = ','.join(fields)


def request_semantic_scholar(title, fields: str = 'title,authors,venue,year,openAccessPdf',
                             url: str = 'https://api.semanticscholar.org/graph/v1/paper/search',
                             api_key=None) -> dict:
    """
    Realiza una solicitud a la API de Semantic Scholar para buscar papers basados en el título proporcionado.

    La función permite especificar campos adicionales para la búsqueda, personalizar la URL del endpoint,
    e incluir una API key para la autenticación. Si se proporciona una API key, esta se incluye en los encabezados
    de la solicitud para permitir un mayor límite de solicitudes y obtener soporte adicional.

    Args:
        title (str): El título del paper que se busca. Este parámetro es obligatorio y se utiliza para filtrar los resultados.
        fields (str, optional): Cadena separada por comas de los campos específicos que se desean retornar en la respuesta.
            Por defecto, se incluyen 'title', 'authors', 'venue', 'year', 'openAccessPdf'.
        url (str, optional): URL del endpoint de la API de Semantic Scholar a la cual se hace la solicitud.
            Por defecto, se usa el endpoint de búsqueda de papers.
        api_key (str, optional): Clave de API para la autenticación en la API de Semantic Scholar.
            Proporcionar una API key permite evitar los límites compartidos de solicitudes para usuarios no autenticados.

    Returns:
        dict: Un diccionario que contiene la respuesta JSON de la API de Semantic Scholar. La estructura del diccionario
            depende de los campos solicitados y de la respuesta de la API.
    """
    # Define los parámetros de la consulta para la solicitud.
    query_params = {
        'query': title,  # Título del paper a buscar.
        'fields': fields,  # Campos específicos a retornar en la respuesta.
        # Limita el número de resultados retornados a 5 para mantener la respuesta manejable.
        'limit': '3',
        # 'openAccessPdf':'' # No funciona. De cualquier manera, podría sernos útil obtener la info del paper aunque no sea de acceso abierto
    }

    # Configura los encabezados de la solicitud, incluyendo la API key si se proporciona.
    headers = {'x-api-key': api_key} if api_key else {}

    # Realiza la solicitud a la API de Semantic Scholar.
    response = get_request(url, params=query_params, headers=headers)
    
    res = response.json()
    res['status_code'] = response.status_code

    # Verifica el código de estado de la respuesta.
    if res['status_code'] != 200:
        print(f"Failed request for: {title}")
            

    # Retorna la respuesta en formato JSON.
    return res
