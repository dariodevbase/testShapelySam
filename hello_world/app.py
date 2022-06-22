import json

import boto3 as boto3
import urllib3 as urllib3
from shapely.geometry import shape
from shapely.geometry import mapping
import shapely



client = boto3.client('ssm')

def lambda_handler(event, context):
    """main handler

    :param event:
    :param context: None
    :return:
    """
    print("event", event)

    url_v1_geospatial_list = "https://api.farmmarketid.com/api/v1/geospatial/land/list"

    url_v1_land_soil = "https://api.farmmarketid.com/api/v1/land/{fmid_land_id}/soil"

    param = client.get_parameter(Name='URL_V1_GEOSPATIAL')
    url_v1_geospatial = str(param['Parameter']['Value'])

    param_key = client.get_parameter(Name='API_KEY_TEST')
    api_key_test = str(param_key['Parameter']['Value'])

    param_key = client.get_parameter(Name='API_KEY_PROD')
    api_key_prod = str(param_key['Parameter']['Value'])

    # Local
    # f = open('/Users/darioastorga/cashrent-serverless/events/data_3_poligonos.json')
    # data = json.load(f)
    # geometry_input = data['features'][0]['geometry']


    # APIgateway
    data = json.loads(event["body"])
    geometry_input = data['geometry']




    lon, lat, meters = get_geometry_data(geometry_input)

    collection_land_details = get_v1_land(lat, lon, meters, api_key_prod)

    FMID_Land_Ids = create_FMID_Land_Id_array(collection_land_details)

    geo_json = get_geospatial_land_list(FMID_Land_Ids,url_v1_geospatial_list, api_key_prod)

    geometry = shape(geometry_input)


    array = []
    data_set = {"type": "FeatureCollection"}

    for feature in geo_json[0]["features"]:
        result = geometry.intersection(shape(feature["geometry"]))

        diccionario = {"type": "Feature", "geometry": "", "properties": {"parentId": data["properties"]["id"]}}

        if not result.is_empty:
            m = mapping(result)
            diccionario['geometry'] = m
            array.append(diccionario)

    internal_marks = {'features': array}
    data_set.update(internal_marks)


    # get_land_soil(FMID_Land_Ids, api_key_prod, url_v1_land_soil)
    # Version de armado antiguo
    # feature_collections = get_GeoJSON(collection_land_details, url_v1_geospatial, api_key_prod)
    # json_dump = create_feature_collection(feature_collections)

    return {
        "headers": {'Content-Type': 'application/json'},
        "statusCode": 200,
        "body": json.dumps(data_set["features"])
    }


def create_FMID_Land_Id_array(collection_land_details):

    temp = []
    for land_details in collection_land_details:
        land_id = land_details['FMID_Land_Id']
        temp.append({"id": land_id})

    # print(temp)

    return temp


def get_land_soil(FMID_Land_Ids, api_key_prod, url_v1_land_soil):

    for land_Id in FMID_Land_Ids:
        # print(land_Id)
        url = url_v1_land_soil.format(fmid_land_id=land_Id['id'])

        headers = {
            'Content-Type': 'application/json',
            'API-Key': api_key_prod
        }

        http = urllib3.PoolManager()
        response = http.request('GET', url, headers=headers)
        result = json.loads(response.data.decode('utf-8'))

        # print(result)

    return result


def get_geospatial_land_list(FMID_Land_Ids, url_v1_geospatial_list, api_key_prod):

    headers = {
        'Content-Type': 'application/json',
        'API-Key': api_key_prod
    }

    encoded_data = json.dumps(FMID_Land_Ids).encode('utf-8')

    http = urllib3.PoolManager()
    response = http.request('POST', url_v1_geospatial_list, headers=headers, body=encoded_data)
    result = json.loads(response.data.decode('utf-8'))

    # print(result)

    return result

def get_GeoJSON(collection_land_details, url_v1_geospatial, api_key_prod):
    feature_collections = ""

    if len(collection_land_details) == 1:

        land_id = str(collection_land_details[0].land_details['FMID_Land_Id'])
        url_v1_land = url_v1_geospatial + land_id
        headers = {
            'API-Key': api_key_prod
        }
        http = urllib3.PoolManager()
        response = http.request('GET', url_v1_land, headers=headers)
        result = json.loads(response.data.decode('utf-8'))
        print(result)

        return result

    else:

        for land_details in collection_land_details:
            land_id = str(land_details['FMID_Land_Id'])
            url_v1_land = url_v1_geospatial + land_id
            headers = {
                'API-Key': api_key_prod
            }
            http = urllib3.PoolManager()
            response = http.request('GET', url_v1_land, headers=headers)
            result = json.loads(response.data.decode('utf-8'))

            # print(result[0]["features"][0])
            feature_collections += str(result[0]["features"][0]) + ","

        return feature_collections


def get_v1_land(lat, lon, meters, api_key_prod):
    url_v1_land = f'https://api.farmmarketid.com/api/v1/land/lat/{lat}/lon/{lon}/radius/{meters}/'

    headers = {
        'API-Key': api_key_prod
    }
    http = urllib3.PoolManager()
    response = http.request('GET', url_v1_land, headers=headers)
    array_result = json.loads(response.data.decode('utf-8'))
    print(array_result)
    return array_result


def get_geometry_data(d):
    # sacamos el punto centro de la geometria
    center_polygon_point = shapely.geometry.shape(d).centroid
    print("center_polygon_point: ", center_polygon_point)

    lon = center_polygon_point.x
    print("lon: ", center_polygon_point.x)

    lat = center_polygon_point.y
    print("lat: ", center_polygon_point.y)

    # sacamos la caja exterior de la geometria
    poligon_outside_area = shapely.geometry.box(*shapely.geometry.shape(d).bounds)
    print("poligon_outside_area: ", poligon_outside_area)

    # sacamos un punto de la caja exterior
    pnt_outside = list(poligon_outside_area.exterior.coords)[0]
    print("pnt_outside: ", pnt_outside)

    # creamos un Point
    new_point = {"type": "Point", "coordinates": pnt_outside}
    p = shapely.geometry.shape(new_point)

    # calculamos la distancia entre el punto de la caja exterior y el punto centro de la geometria
    meters = int(p.distance(center_polygon_point) * 100000)
    print("meters: ", meters)

    return lon, lat, meters


def create_feature_collection(feature_collections):

    data_set = {"type": "FeatureCollection", "features": [feature_collections]}
    json_dump = json.dumps(data_set)

    return json_dump
