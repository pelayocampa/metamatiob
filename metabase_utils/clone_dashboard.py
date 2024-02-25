"""Clone Dashboard

Implementation over MetabaseAPI to clone dashboards, queries and filters
without the needing of the pro version.

"""

import concurrent.futures
import csv
import datetime
import json
import logging
import os
import pathlib
import re
import time

from dotenv import load_dotenv
from metabase_api import Metabase_API
from tqdm import tqdm

from metabase_utils.database_fields import FieldMatcher


class ElementNotFound(Exception):
    """Exception to be raised when an Element is not found

    Args:
        Exception (_type_): A general exception
    """


class MetabaseError(Exception):
    """_summary_

    Args:
        Exception (_type_): _description_

    Raises:
        MetabaseE: _description_
        Exception: _description_
        ElementNotFound: _description_
        ElementNotFound: _description_
        Exception: _description_
        ElementNotFound: _description_
        Exception: _description_
        ElementNotFound: _description_
        ElementNotFound: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_

    Returns:
        _type_: _description_
    """


def get_collections_id_re(
    metabase_instance: Metabase_API, collection_match: str, **kwargs
) -> list:
    """Searchs collection id from a regular expression.

    Args:
        metabase_instance (Metabase_API): Metabase handler.
        collection_match (str): regex to match.
        location (str): Location of the collection. Default, /

    Returns:
        list: List of all the matching items.
    """

    location = kwargs.get("location")
    index = kwargs.get("index")
    match_location = kwargs.get("match_location")

    if not location:
        location = "/"
    if not index:
        index = "name"
    elif index not in ["name", "slug"]:
        raise Exception("Invalid index, expected 'name' of 'slug'")

    if match_location is None:
        match_location = True

    regexp = re.compile(collection_match)
    response = metabase_instance.get("/api/collection/")

    matching = []

    if response:
        for collection in response:
            text = collection.get(index)
            if text is None:
                continue
            match = re.match(regexp, text)
            if collection.get("location") == location:
                (
                    matching.append(collection)
                    if match and match_location
                    else None
                )
            else:
                (
                    matching.append(collection)
                    if match and not match_location
                    else None
                )
    return matching if len(matching) > 0 else None


def get_collection_name(
    metabase_instance: Metabase_API, collection_id: str
) -> str:
    """Returns the name of a collection refered by its ID

    Args:
        metabase_instance (Metabase_API): Metabase handler
        collection_id (str): _description_

    Returns:
        str: _description_
    """
    response = metabase_instance.get(f"/api/collection/{collection_id}")

    return response.get("name") if response else "Not found"


def get_collection_id(
    metabase_instance: Metabase_API,
    collection_name: str,
    path="/",
    create=False,
) -> str:
    """
    Extracts the collection id from a metabase instance given its name
    : param metabase_instance: metabase instance for (Metabase_API object)
    : param database_name: collection name in the instance
    : returns: id of the collection
    : raises ElementNotFound: If the collection is not present in the instance
    """
    response = metabase_instance.get("/api/collection")
    if response:
        for collection in response:
            if collection["name"].lower() == collection_name.lower():
                if collection["location"] == path:
                    return collection["id"]
        if create:
            request = metabase_instance.post(
                "/api/collection/",
                json={
                    "name": collection_name,
                    "parent_id": int(path.strip("/")) if path != "/" else None,
                    "color": "#282936",
                },
            )
            return request["id"]
    return None


def get_dashboard_id(
    metabase_instance: Metabase_API, dashboard_name: str
) -> str:
    """
    Extracts the dashboard id from a metabase instance given its name
    : param metabase_instance: metabase instance for (Metabase_API object)
    : param dashboard_name: dashboard name in the instance
    : returns: id of the dashboard
    : raises ElementNotFound: If the dashboard is not present in the instance
    """
    response = metabase_instance.get("/api/dashboard")
    if response:
        for dashboard in response:
            if dashboard["name"].lower() == dashboard_name.lower():
                return dashboard["id"]
    raise ElementNotFound(
        f"""Dashboard {dashboard_name} not found in metabase
        {metabase_instance.domain}"""
    )


def get_dashboard_name(
    metabase_instance: Metabase_API, dashboard_id: str
) -> str:
    """Get dashboard name from id.

    Args:
        metabase_instance (Metabase_API): Metabase instance to retrieve name
        dashboard_id (str): id from dashboard

    Returns:
        str: Dashboard name if id exists, None in other case.
    """
    response = metabase_instance.get(f"/api/dashboard/{dashboard_id}")

    if response:
        name = response.get("name")
        return name if name else None
    raise ElementNotFound(
        f"Dashboard {dashboard_id} not found in {metabase_instance.domain}"
    )


def get_elements_from_collection(
    metabase_instance: Metabase_API, collection_id: str
) -> dict:
    """Get all the elements inside a collection.

    Args:
        metabase_instance (Metabase_API): Metabase instance handler.
        collection_id (str): Collection identifier.

    Returns:
        dict: All the entries in the collection: dashboards, questions, other collections.
    """
    response = metabase_instance.get(f"/api/collection/{collection_id}/items")
    if response:
        return response
    else:
        raise Exception(f"Collection {collection_id} not available")


def get_dashboards_from_collection(
    metabase_instance: Metabase_API, collection_id: int
) -> dict:
    """Retrieves all dashboards inside a collection.

    Args:
        metabase_instance (Metabase_API): Metabase instance handler.
        collection_id (int): Collection identifier

    Returns:
        dict: Dictionary providing different dashboards inside that collection.
    """
    response = metabase_instance.get(
        f"/api/collection/{collection_id}/items?models=dashboard"
    )
    return response


def get_database_id(
    metabase_instance: Metabase_API, database_name=None, enterprise_name=None
) -> str:
    """
    Extracts the database id from a metabase instance given its name
    : param metabase_instance: metabase instance for (Metabase_API object)
    : param database_name: database name in the instance
    : param enterpise_name: just in case we're using a name for database different than instance name.
    : returns: id of the database
    : raises ElementNotFound: If the database is not present in the instance
    """
    database_json = metabase_instance.get("/api/database")
    database_name = database_name.lower() if database_name else None
    enterprise_name = enterprise_name.lower() if enterprise_name else None

    for database in database_json["data"]:
        if database["name"].lower() in [
            database_name,
            enterprise_name,
        ]:
            return database["id"]
    raise ElementNotFound(
        f"Database {database_name} not found in metabase {metabase_instance.domain}"
    )


def get_instance_name(metabase_instance: Metabase_API) -> str:
    """
    Extracts the domain part of the metabase host.

    e.g: db in https://db.seetransparent.com
         localhost in http://localhost:3000
    : param metabase_instance: metabase instance for (Metabase_API object)
    : returns: name of the instance
    """
    res = re.search(r"http(s)?://(\w+)(.\w+)*", metabase_instance.domain)
    if res:
        return res.group(2)
    raise Exception(f"Not matched on {metabase_instance.domain}")


def get_clean_queries(
    queries: dict,
    old_database_name: str,
    client_type=None,
    client_id=None,
):
    """Clean a query set. If source database name appears in source queries, remove it.

    Also works cleaning pm_domain_name and owner_id
    Args:
        queries (dict): Not cleaned queries
        old_database (str): Source database name
        type (_type_, optional): Type pm_domain_name or owner_id. Mandatory. Defaults to None.
        client_id (_type_, optional): ID of client. Mandatory. Defaults to None.
    """

    if client_type == "enterprise":
        return queries

    if client_id is None:
        raise ElementNotFound("You should provide client_id")

    re_owner = r"[`]?pm_domain_name[`]?(\s)*(!?=)(\s)*'[\S]+'"
    match client_type:
        case "pm_domain_name":
            sub_text = r"`pm_domain_name` \2 '" + client_id + "'"
        case "owner_id":
            sub_text = r"`owner_id` \2 '" + client_id + "'"
        case _:
            raise ElementNotFound(
                "No type set. You should specify: \
                                  pm_domain_name or owner_id as type"
            )
    for key in queries:
        try:
            query = queries[key]["dataset_query"]["native"]["query"]
            query = re.sub(f"(`)?{old_database_name}(`)?.", "", query)
            query = re.sub(re_owner, sub_text, query)
            queries[key]["dataset_query"]["native"]["query"] = query

        except KeyError:
            # Some cards do not have these keys (text cards)
            continue
    return queries


def get_info(
    metabase_instance: Metabase_API,
    dashboard_id: int,
    pm_domain_name: str,
    owner_id: str,
    enterprise_name: str,
) -> dict:
    """
    Get a list of the standard filter parameters
    : param source_metabase_instance: Metabase_API instanced of the source metabase instance
    : param dashboard_name: Dashboard name to be cloned
    : param pm_domain_name: set to format data on pm data
    : param owner_id: set to format data on owner_id data
    : returns: a dictionary containing the necessary queries to recreate the dashboard
    """
    response = get_dashboard(metabase_instance, dashboard_id)
    ordered_cards = response["ordered_cards"]
    queries = {}
    for origin_card in tqdm(
        ordered_cards,
        leave=False,
        desc=f"Getting cards from dashboard {dashboard_id}",
    ):
        card_id = origin_card["id"]
        if len(origin_card["card"]) <= 1:  # Markdown card
            queries[card_id] = {
                "visualization_settings": origin_card[
                    "visualization_settings"
                ],
                "col": origin_card["col"],
                "row": origin_card["row"],
                "sizeX": origin_card["sizeX"],
                "sizeY": origin_card["sizeY"],
            }
        else:
            card_dict = origin_card["card"]
            query = card_dict["dataset_query"]
            visual_settings = card_dict["visualization_settings"]
            parameter_mappings = origin_card["parameter_mappings"]  # Is a dict
            queries[card_id] = {
                "name": card_dict["name"],
                "dataset_query": query,
                "visualization_settings": visual_settings,
                "display": card_dict["display"],
                "col": origin_card["col"],
                "row": origin_card["row"],
                "sizeY": origin_card["sizeY"],
                "sizeX": origin_card["sizeX"],
                "parameter_mappings": parameter_mappings,
            }
    if pm_domain_name:
        return get_clean_queries(
            queries,
            old_database_name=get_instance_name(metabase_instance),
            client_type="pm_domain_name",
            client_id=pm_domain_name,
        )
    elif owner_id:
        return get_clean_queries(
            queries,
            old_database_name=get_instance_name(metabase_instance),
            client_type="owner_id",
            client_id=owner_id,
        )
    elif enterprise_name:
        return get_clean_queries(
            queries,
            old_database_name=enterprise_name,
            client_type="enterprise",
            client_id=enterprise_name,
        )
    return queries


def get_dashboard(metabase_instance: Metabase_API, dashboard_id, reload=False):
    """Retrieves a dashboard in the instance. If the dashboard is cached
    it reads the file. Using reload you can refresh the cache.

    Args:
        metabase_instance (Metabase_API): Initialized metabase instance to read from
        dashboard_id (_type_): _description_
        reload (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """
    instance_name = get_instance_name(metabase_instance=metabase_instance)
    dashboard_file_path = f"./config/cards/{instance_name}"

    os.makedirs(dashboard_file_path, exist_ok=True)

    dashboard_file_path = os.path.join(
        dashboard_file_path, f"dashboard_{dashboard_id}.json"
    )

    logging.info(
        "Getting from %s/api/dashboard/%s",
        metabase_instance.domain,
        dashboard_id,
    )
    if (
        reload
        or not os.path.isfile(dashboard_file_path)
        or (os.stat(dashboard_file_path).st_size == 0)
        or (os.path.getmtime(dashboard_file_path) < time.time() - 24 * 3600)
    ):  # older than yesterday
        response = metabase_instance.get(f"/api/dashboard/{dashboard_id}")
        with open(
            dashboard_file_path, "w", encoding="utf-8"
        ) as dashboard_file:
            json.dump(response, dashboard_file, indent=4)
    else:
        with open(
            dashboard_file_path, "r", encoding="utf-8"
        ) as dashboard_file:
            response = json.load(dashboard_file)
    return response


def unify_str(ugly_string: str) -> str:
    return re.sub(" +", "_", ugly_string).lower()


def unify_key_names(not_enough_good_dict: dict) -> dict:
    """
    Convert keys into the string_string_string format. That way you can easily
    compare them and check for matches between differente files.


    Args:
        not_enough_good_dict (dict): A dictionary with long key name spaced

    Returns:
        dict: A common dictionary lowcased and _ insted of spaces.
    """

    unified_dict = {
        unify_str(x): not_enough_good_dict.get(x)
        for x in not_enough_good_dict.keys()
    }
    return unified_dict


def dump_fields_on_database_by_id(
    database_id: int, metabase_instance: Metabase_API
) -> dict:
    """Gets all the fields on the database.

    Args:
        database_id (int): A valid database identifier on Metabase instance
        metabase_instance (Metabase_API): Metabase instance to check

    Returns:
        dict: _description_
    """
    field_list = metabase_instance.get(f"/api/database/{database_id}/fields")
    database_name = metabase_instance.get(f"/api/database/{database_id}")[
        "name"
    ]
    field_list = [
        p
        for p in field_list
        if p["schema"] == database_name
        or p["schema"] == database_name.lower()
        or p["schema"] == "default"
        or p["schema"] == "new_zealand"
        or p["schema"] == "rio_de_janeiro"
        or p["schema"] == "canarias"
    ]

    if len(field_list) == 0:
        raise Exception("Invalid database {}".format(database_name))

    different_tables = set()

    for current_field in field_list:
        different_tables.add(current_field["table_name"])
    dict_fields_by_table = {}

    for table_name_entry in different_tables:
        dict_fields_by_table[table_name_entry] = {}
        for current_field in field_list:
            if current_field["table_name"] == table_name_entry:
                dict_fields_by_table[table_name_entry][
                    current_field["name"]
                ] = current_field["id"]
    return dict_fields_by_table


def rename_parameter_mappings(
    input_dictionary: dict, bastard_id_from_hell: str, replacement: str
) -> dict:
    """Does a match between fields using set intersection.

    Args:
        input (dict):
        card_id_to_replace (str): _description_
        bastard_id_from_hell (str): _description_
        replacement (str): _description_

    Returns:
        dict: A new valid dictionary for PUT request in metabase.
    """
    renamed_dict = {
        "id": bastard_id_from_hell,
        "row": input_dictionary["row"],
        "col": input_dictionary["col"],
        "sizeX": input_dictionary["sizeX"],
        "sizeY": input_dictionary["sizeY"],
        "visualization_settings": input_dictionary["visualization_settings"],
        "series": input_dictionary["series"],
        "parameter_mappings": [],
    }

    for element in input_dictionary["parameter_mappings"]:
        temp_dictionary = {
            "parameter_id": element["parameter_id"],
            "card_id": replacement,
            "target": element["target"],
        }
        renamed_dict["parameter_mappings"].append(temp_dictionary)
    return renamed_dict


def get_available_collections_and_dashboards(
    source_metabase: Metabase_API,
) -> dict:
    """

    Args:
        source_metabase (Metabase_API): _description_

    Returns:
        dict: _description_
    """
    collections_and_dashboards_dict = {}

    collection_list = source_metabase.get("""/api/collection""")

    for collection in tqdm(collection_list):
        collection_dashboards = source_metabase.get(
            f"""/api/collection/{collection['id']}/items?models=dashboard"""
        )
        collections_and_dashboards_dict[
            collection["id"]
        ] = collection_dashboards
    return collections_and_dashboards_dict


def create_new(
    source_metabase: Metabase_API,
    destination_metabase: Metabase_API,
    queries: dict,
    source_dashboard_id: int,
    collection_id=None,
    dashboard_name=None,
    type=None,
):
    """
    Create the new dashboard containing all the info copied from the source
     dashboard
    : param metabase: Metabase_API instance with the connected
      destination metabase instance
    : param dashboard_name: Name of the new Dashboard to be created
    : param queries: Dictionary containing all the info needed to
      create all the cards in the new dashboard
    : returns: None
    """

    source_collection_json = None
    source_collection_id = get_dashboard(
        metabase_instance=source_metabase, dashboard_id=source_dashboard_id
    )["collection_id"]

    if source_collection_id:
        source_collection_json = source_metabase.get(
            f"/api/collection/{source_collection_id}"
        )

    source_collection_name = (
        source_collection_json["name"] if source_collection_json else "Custom"
    )

    if not collection_id:
        dashboard_collection_id = get_collection_id(
            metabase_instance=destination_metabase,
            collection_name="Dashboards",
            create=True,
        )
        # parent_collection = source_collection_json.get('parent_collection')
        collection_id = get_collection_id(
            metabase_instance=destination_metabase,
            collection_name=source_collection_name,
            path=f"/{dashboard_collection_id}/",
            create=True,
        )

    # filter_params = get_filter_params(metabase_instance=source_metabase, dashboard_id=source_dashboard_id)
    filter_params = check_filter_params_exists(
        source_metabase=source_metabase, source_dashboard=source_dashboard_id
    )

    if not dashboard_name:
        dashboard_name = get_dashboard_name(
            metabase_instance=source_metabase, dashboard_id=source_dashboard_id
        )

    # old_version = get_old_dashboard_version(metabase_instance=destination_metabase, dashboard_name=dashboard_name)
    """
    if old_version:
        source_list = old_version['parameters']

        for destination_parameters in filter_params:
            changed = False
            for source_parameters in source_list:
                if changed:
                    break
                if destination_parameters['slug'] == source_parameters['slug']:
                    source_default = source_parameters.get('default')
                    destination_parameters['default'] = source_default if source_default else None
                    changed = True"""

    dash_params = {
        "name": dashboard_name,
        "collection_id": collection_id,
        "parameters": filter_params,
    }
    response = destination_metabase.post("/api/dashboard/", json=dash_params)
    created_dashboard_id = response["id"]  # Id of new dashboard created

    logging.info(
        "Dashboard %s created with id %s", dashboard_name, created_dashboard_id
    )

    collection_id = get_collection_id(
        destination_metabase, "Questions", create=True
    )
    # timestamp = datetime.datetime.now().strftime('%d-%m-%Y')

    questions_location = get_collection_id(
        metabase_instance=destination_metabase,
        path=f"/{collection_id}/",
        collection_name=source_collection_name,
        create=True,
    )

    dashboard_code = re.match(r"^([A-Z].?\d+).*", dashboard_name)
    payload = {
        "name": (
            f"{dashboard_code.group(1)} [{created_dashboard_id}]"
            if dashboard_code
            else f"{created_dashboard_id}"
        ),
        "color": "#000500",
        "parent_id": questions_location,
    }
    questions_location = destination_metabase.post(
        """/api/collection/""", json=payload
    )["id"]

    populate_cards(
        destination_metabase, queries, created_dashboard_id, questions_location
    )

    response = connect_filters(
        source_metabase=source_metabase,
        destination_metabase=destination_metabase,
        source_dashboard_id=source_dashboard_id,
        created_dashboard_id=created_dashboard_id,
        # dashboard_dict=response,
    )

    # Well done Mr Sirloin.
    # If we get the 200 response we asume we are doing a succesfull insert

    if response == 200:
        logging.info(
            "Dashboard (id: %s) %s populated",
            created_dashboard_id,
            dashboard_name,
        )


def populate_single_card(
    destination_metabase: Metabase_API,
    card_to_copy: dict,
    created_dashboard_id,
    questions_location,
    tqdm_queries,
):
    if "name" in card_to_copy:
        card_name = card_to_copy["name"]

        card_params = {
            "visualization_settings": card_to_copy["visualization_settings"],
            "collection_id": questions_location,
            "name": card_name,
            "dataset_query": card_to_copy["dataset_query"],
            "display": card_to_copy["display"],
        }  # Card params

        new_card = destination_metabase.post(
            "/api/card", json=card_params
        )  # Creating card
        card_id = new_card["id"]

        card_adding_json = {
            "cardId": card_id,
            "parameter_mappings": [],
            "sizeX": card_to_copy["sizeX"],
            "sizeY": card_to_copy["sizeY"],
            "col": card_to_copy["col"],
            "row": card_to_copy["row"],
        }
        tqdm_queries.set_description(f"Creating {card_name}")
        destination_metabase.post(
            f"/api/dashboard/{created_dashboard_id}/cards",
            json=card_adding_json,
        )

    else:
        card_adding_json = {
            "sizeX": card_to_copy["sizeX"],
            "sizeY": card_to_copy["sizeY"],
            "col": card_to_copy["col"],
            "row": card_to_copy["row"],
            "visualization_settings": card_to_copy["visualization_settings"],
        }
        tqdm_queries.set_description("Creating an unnamed card")
        destination_metabase.post(
            f"/api/dashboard/{created_dashboard_id}/cards",
            json=card_adding_json,
        )
        # cache_dashboard(dashboard_dict=response, dashboard_id=created_dashboard_id,
        #                metabase_instance_name=get_instance_name(metabase_instance=destination_metabase))


def populate_cards(
    destination_metabase: Metabase_API,
    queries,
    created_dashboard_id,
    questions_location,
):
    tqdm_queries = tqdm(queries, leave=False)
    futures = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for k in tqdm_queries:
            card_to_copy = queries[k]
            futures.append(
                executor.submit(
                    populate_single_card,
                    destination_metabase=destination_metabase,
                    card_to_copy=card_to_copy,
                    created_dashboard_id=created_dashboard_id,
                    questions_location=questions_location,
                    tqdm_queries=tqdm_queries,
                )
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(e)
            else:
                tqdm_queries.set_description("")


def cache_dashboard(
    dashboard_dict: dict, dashboard_id: int, metabase_instance_name: str
) -> None:
    cache_destination = f"./config/cards/{metabase_instance_name}/dashboard_{dashboard_id}.json"
    with open(cache_destination, "w") as cache_file:
        json.dump(dashboard_dict, cache_file, indent=4)


def connect_filters(
    source_metabase: Metabase_API,
    destination_metabase: Metabase_API,
    source_dashboard_id: str,
    created_dashboard_id: str,
    # dashboard_dict: dict = None,
) -> dict:
    """Connect filters in a metabase instance

    Args:
        metabase (Metabase_API): destination metabase instance to perform changes
        source_dashboard_id (_type_): _description_
        created_dashboard_id (_type_): _description_

    Returns:
        dict: response from the server containing the cards on the dashboard
    """

    destination_dashboard = get_dashboard(
        destination_metabase, created_dashboard_id
    )
    destination_cards = destination_dashboard["ordered_cards"]

    source_dashboard = get_dashboard(source_metabase, source_dashboard_id)

    source_cards = source_dashboard["ordered_cards"]
    destination_dashboard_parameters = []

    for destination_card in tqdm(destination_cards, leave=False):
        card_data = destination_card["card"]
        card_name = card_data.get("name")

        if card_name is None:
            continue

        for source_card in source_cards:
            source_card_data = source_card["card"]

            source_card_name = source_card_data.get("name")

            if source_card_name is None:
                continue

            if source_card_name == card_name:
                if (
                    source_card["sizeX"] == destination_card["sizeX"]
                    and source_card["sizeY"] == destination_card["sizeY"]
                    and source_card["col"] == destination_card["col"]
                    and source_card["row"] == destination_card["row"]
                ):
                    destination_dashboard_parameters.append(
                        rename_parameter_mappings(
                            input_dictionary=source_card,
                            bastard_id_from_hell=destination_card[
                                "id"
                            ],  # id, different than question id
                            replacement=destination_card["card_id"],
                        )
                    )  # Question id (used in question/id), card_id
    destination_dashboard_parameters = {
        "cards": destination_dashboard_parameters
    }
    response = destination_metabase.put(
        f"/api/dashboard/{created_dashboard_id}/cards",
        json=destination_dashboard_parameters,
    )

    # get_old_dashboard_versions(metabase_instance=metabase,
    #                           dashboard_name=destination_dashboard['name'])
    return response


def give_me_instance_dashboards_id_name(
    metabase_instance: Metabase_API, collection_id: int
) -> dict:
    """Returns a list of dashboards in a collection.

    Args:
        metabase_instance (Metabase_API): metabase api to connect
        collection_id (int): collection id

    Returns:
        dict: _description_
    """
    return_fancy_dict = {}
    payload = ["dashboard"]
    dashboard_dictionary = metabase_instance.get(
        f"""/api/collection/{collection_id}/items""", json=payload
    )
    for element in dashboard_dictionary["data"]:
        if element["model"] == "dashboard":
            return_fancy_dict[element["id"]] = element["name"]

    return return_fancy_dict


def clone_multiple_dashboards(
    source_metabase: Metabase_API,
    destination_metabase: Metabase_API,
    source_list: list,
    pm_domain_name=None,
    owner_id=None,
    enterprise=None,
    destination_database=None,
):
    """_summary_

    Args:
        source_metabase (Metabase_API): _description_
        destination_metabase (Metabase_API): _description_
        source_list (list): _description_
        pm_domain_name (_type_, optional): _description_. Defaults to None.
        owner_id (_type_, optional): _description_. Defaults to None.
        enterprise (_type_, optional): _description_. Defaults to None.
    """

    futures = []
    dashboards = tqdm(source_list, leave=False) if source_list else None

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for dashboard in dashboards:
            dashboards.set_description(f"Retrieving dashboard: {dashboard}")
            futures.append(
                executor.submit(
                    create_and_link_dashboard,
                    source_metabase=source_metabase,
                    destination_metabase=destination_metabase,
                    pm_domain_name=pm_domain_name,
                    owner_id=owner_id,
                    enterprise=enterprise,
                    source_dashboard=str(dashboard),
                    destination_database=destination_database,
                )
            )
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(e)
            else:
                logging.info("Done")


def clone_collection_on_collection(
    source_metabase: Metabase_API,
    destination_metabase: Metabase_API,
    source_collection: str,
    destination_collection=None,
    pm_domain_name=None,
    owner_id=None,
    enterprise=None,
    destination_database=None,
    backup=False,
):
    """Doing some tests on enterprise collections

    Args:
        source_metabase (Metabase_API): source metabase instance
        destination_metabase (Metabase_API): destinations metabase instance
        source_collection (str): id of the source collection
        destination_collection (str): id of the destination collection
    """

    source_dashboards = get_dashboards_from_collection(
        metabase_instance=source_metabase, collection_id=source_collection
    )

    if destination_collection is None:
        destination_collection = get_collection_id(
            destination_metabase,
            f"""{get_collection_name(
                metabase_instance=source_metabase, collection_id=source_collection
            )}{'backup' if backup is True else ''}""",
            create=True,
        )

    futures = []
    dashboards_tqdm = tqdm(source_dashboards["data"], leave=False)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for source_dashboard in dashboards_tqdm:
            dashboards_tqdm.set_description(
                f'Creating {source_dashboard["name"]} in {destination_metabase.domain}'
            )
            futures.append(
                executor.submit(
                    create_and_link_dashboard,
                    source_metabase=source_metabase,
                    destination_metabase=destination_metabase,
                    pm_domain_name=pm_domain_name,
                    owner_id=owner_id,
                    enterprise=enterprise,
                    source_dashboard=source_dashboard,
                    destination_database=destination_database,
                )
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(e)
            else:
                logging.info("Done")


def table_display_name_to_name(
    metabase_instance: Metabase_API = None, database_name: str = None
) -> dict:
    """Returns a dictionary matching display_name with table_name and vice versa

    Args:
        metabase_instance (Metabase_API, optional): _description_. Defaults to None.
    """
    tables = metabase_instance.get("/api/table")

    country_name = None

    if database_name:
        pattern = re.compile(r"([\w\d]+)\s*Data", flags=re.IGNORECASE)
        country_name = re.search(pattern, database_name)

    display_name_to_name = {}
    name_to_display_name = {}

    for element in tables:
        display_name_to_name[element["display_name"]] = element["name"]
        element_name = (
            re.sub(
                rf"\_{country_name.group(1)}(\_?)",
                r"\g<1>",
                element["name"],
                flags=re.IGNORECASE,
            )
            if country_name is not None
            else element["name"]
        )
        name_to_display_name[element_name] = element["display_name"]

    return {
        "to_name": display_name_to_name,
        "to_display_name": name_to_display_name,
    }


def check_tables(
    metabase_instance: Metabase_API,
    destination_database: str = None,
    queries: dict = None,
) -> bool:
    """Check available tables in destination

    Args:
        metabase_instance (Metabase_API): destination metabase.
        queries (dict): queries from source.

    Returns:
        bool: True if all tables are available. False if it's something missing
    """

    tables_on_source = table_display_name_to_name(
        metabase_instance=metabase_instance, database_name=destination_database
    )

    valid = True

    for x in queries:
        name = queries[x].get("name")
        if name:
            needed_table = re.search(
                r"FROM\s+\W?(\w+|\d+)\W?\s",
                queries[x]["dataset_query"]["native"]["query"],
            ).group(1)

            if unify_str(needed_table) not in unify_key_names(
                tables_on_source["to_name"],
            ):
                logging.warning("%s needed but not found", needed_table)
                valid = False
    return valid


def create_and_link_dashboard(
    source_metabase: Metabase_API,
    destination_metabase: Metabase_API,
    pm_domain_name: str,
    owner_id: str,
    enterprise: str,
    source_dashboard: str,
    rename_dashboard=None,
    destination_database=None,
):
    """_summary_

    Args:
        source_metabase (Metabase_API): _description_
        destination_metabase (Metabase_API): _description_
        pm_domain_name (str): _description_
        owner_id (str): _description_
        enterprise (str): _description_
        origin_fields (dict): _description_
        dest_fields (dict): _description_
        source_dashboard (str): _description_
        rename_dashboard (_type_, optional): _description_. Defaults to None.
    """
    queries = get_info(
        metabase_instance=source_metabase,
        dashboard_id=(
            source_dashboard["id"]
            if isinstance(source_dashboard, dict)
            else source_dashboard
        ),
        # Checking if we're receiving a dict of an int
        pm_domain_name=pm_domain_name,
        owner_id=owner_id,
        enterprise_name=enterprise,
    )

    if (
        check_tables(
            metabase_instance=destination_metabase,
            destination_database=destination_database,
            queries=queries,
        )
        is False
    ):
        logging.warning(
            f"""Cannot copy {source_dashboard[
            "id"] if isinstance(source_dashboard, dict) else source_dashboard}"""
        )
        return

    if enterprise:
        queries = remap_queries(
            origin_metabase=source_metabase,
            destination_metabase=destination_metabase,
            queries=queries,
            database_name=destination_database,
            enterprise=enterprise,
        )
    else:
        queries = remap_queries(
            origin_metabase=source_metabase,
            destination_metabase=destination_metabase,
            queries=queries,
            database_name=destination_database,
        )

    logging.info("Creating dashboard")

    if rename_dashboard:
        dashboard_name = rename_dashboard
    else:
        dashboard_name = (
            source_dashboard["name"]
            if isinstance(source_dashboard, dict)
            else get_dashboard_name(
                metabase_instance=source_metabase,
                dashboard_id=source_dashboard,
            )
        )

    create_new(
        source_metabase=source_metabase,
        destination_metabase=destination_metabase,
        dashboard_name=dashboard_name,
        #  collection_id=created_collection_id,
        queries=queries,
        source_dashboard_id=(
            source_dashboard["id"]
            if isinstance(source_dashboard, dict)
            else source_dashboard
        ),
    )


def remap_queries(
    origin_metabase: Metabase_API,
    destination_metabase: Metabase_API,
    queries: dict,
    enterprise_name=None,
    database_name=None,
) -> dict:
    """_summary_

    Args:
        origin_fields (dict): _description_
        dest_metabase (Metabase_API): _description_
        dest_fields (dict): _description_
        queries (dict): _description_

    Returns:
        dict: _description_
    """

    logging.info("Reformulating queries")
    destination_database_id = get_database_id(
        destination_metabase,
        database_name=(
            database_name
            if database_name
            else get_instance_name(destination_metabase)
        ),
        enterprise_name=enterprise_name,
    )

    awesome_matching_id_dict = FieldMatcher(
        source_instance=origin_metabase,
        destination_instance=destination_metabase,
    ).matches

    tqdm_queries = tqdm(queries, leave=False)

    for single_query in tqdm_queries:
        tqdm_queries.set_description(
            f'Remapping query {queries[single_query]["name"] if queries[single_query].get("name") else "without name."}'
        )

        dataset_query = queries[single_query].get("dataset_query")
        if dataset_query:
            fields_linked = dataset_query["native"]["template-tags"]

            source_database_id = dataset_query.get("database")

            for current_field in fields_linked:
                dimension = fields_linked[current_field].get("dimension")
                if dimension:
                    id = dimension[1]
                    try:
                        dimension[1] = awesome_matching_id_dict[
                            source_database_id
                        ][destination_database_id][id]
                    except Exception:
                        logging.warning(
                            "ID: %s, field has no match",
                            id,
                        )

            dataset_query["database"] = destination_database_id

    return queries


def check_filter_params_exists(
    source_metabase: Metabase_API, source_dashboard: str
) -> list:
    """Checks the existence of a filter into our json configuration file. In case it cannot
    find it, it retrieves a

    Args:
        pretty_args (_type_): Dictionary with data concerning dashboard source
        source_metabase (_type_): Source metabase to query if it cannot find the filter
    """
    instance_filters_dir = f"./config/{get_instance_name(source_metabase)}"

    os.makedirs(instance_filters_dir, exist_ok=True)
    filter_file = f"{instance_filters_dir}/filter_params.json"

    if (
        not os.path.isfile(filter_file)
        or (os.stat(filter_file).st_size == 0)
        or (os.path.getctime(filter_file) < time.time() - 24 * 3600)
    ):
        with open(filter_file, "w", encoding="utf8") as output_file:
            current_filters = {}
            current_filters[source_dashboard] = source_metabase.get(
                f"""/api/dashboard/{source_dashboard}"""
            )["parameters"]
            json.dump(current_filters, output_file, indent=4)
    else:
        with open(filter_file, "r", encoding="utf8") as input_output_file:
            current_filters = json.load(input_output_file)
            current_dashboard_filters = current_filters.get(
                str(source_dashboard)
            )
            if current_dashboard_filters:
                return current_dashboard_filters
        current_filters[source_dashboard] = source_metabase.get(
            f"""/api/dashboard/{source_dashboard}"""
        )["parameters"]
        with open(filter_file, "w", encoding="utf8") as output_filter_file:
            json.dump(current_filters, output_filter_file, indent=4)

    return current_filters[source_dashboard]


def get_pm_domain_from_csv() -> dict:
    """Reads csv file with configuration fields matching instance name with
    pm_domain name

    Returns:
        dict: A dict with all configuration fields
    """
    pm_domain_from_instance = {}

    pm_domain_from_instance_file = pathlib.Path(
        "./config/domain_to_instance.csv"
    )

    if pm_domain_from_instance_file.exists():
        with pm_domain_from_instance_file.open("r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                instance = re.match(r"^ *(.*).seetransparent.com$", row[2])
                if instance:
                    instance = instance.group(1)
                pm_domain = row[1]
                pm_domain_from_instance[instance] = pm_domain
    return pm_domain_from_instance


def search_pm_domain_from_instance(
    pm_domain_from_instance, dest_instance_url
) -> str:
    """Searchs if pm_domain exists in our list of pm_domains

    Args:
        pm_domain_from_instance (_type_): _description_
        dest_instance_url (_type_): _description_

    Raises:
        Exception: _description_

    Returns:
        str: _description_
    """

    matched = re.match(
        r"^ *https://(.*).seetransparent.com",
        dest_instance_url,
    )
    if matched:
        instance_name = matched.group(1)
        pm_domain_info = pm_domain_from_instance.get(instance_name)
        if pm_domain_info:
            return pm_domain_info
    raise ValueError("No pm_domain_name provided or available in csv file.")


def setup_env() -> tuple:
    logging.info("Loading environment variables")
    load_dotenv()

    url = os.getenv("SOURCE_INSTANCE_URL")
    user = os.getenv("SOURCE_INSTANCE_USER")
    password = os.getenv("SOURCE_INSTANCE_PASSWORD")

    logging.info("Connecting to the metabase instances...")
    source_metabase = Metabase_API(domain=url, email=user, password=password)

    if not source_metabase:
        raise Exception(f"Bad request on source instance {source_metabase}")

    logging.info(
        "Source metabase instance %s connected.", source_metabase.domain
    )

    url = os.getenv("DEST_INSTANCE_URL")
    user = os.getenv("DEST_INSTANCE_USER")
    password = os.getenv("DEST_INSTANCE_PASSWORD")

    destination_metabase = Metabase_API(
        domain=url, email=user, password=password
    )

    if not destination_metabase:
        raise Exception(
            f"Bad request on destination instance {destination_metabase}"
        )

    logging.info(
        "Destination metabase instance `%s` connected.",
        destination_metabase.domain,
    )

    return source_metabase, destination_metabase


def get_old_dashboard_version(
    metabase_instance: Metabase_API, dashboard_name: str
) -> dict:
    """Checks if an old version of a dashboard exists. They must have a very
    similar name: A.5. Test - vX.Y (YYYY-MM) macthing till '-'

    Args:
        metabase_instance (Metabase_API): _description_
        dashboard_name (str): _description_

    Returns:
        list: _description_
    """

    split_dashboard_name_version = re.compile(
        r"([A-Za-z]\.\d\.?\s[\w\s()]+)\s?\-?\s?v\.?(\d\.\d)\s+\((\d{4}\.\d{1,2})\)"
    )
    new_dashboard = re.match(split_dashboard_name_version, dashboard_name)
    response = metabase_instance.get("/api/dashboard/")

    if response:
        name_id = [
            (dashboard.get("name"), dashboard.get("id"))
            for dashboard in response
            if re.match(new_dashboard.group(1), dashboard.get("name"))
        ]

        version_date_id = []
        for item in name_id:
            extract_version = re.match(split_dashboard_name_version, item[0])
            if extract_version:
                version_date_id.append(
                    [
                        extract_version.group(2),
                        extract_version.group(3),
                        item[1],
                    ]
                )
        # For now we check if there's a prior version on the instance.

        previous_version = [
            match
            for match in version_date_id
            if int(new_dashboard.group(2).replace(".", "")) - 1
            == int(match[0].replace(".", ""))
        ]

        # Getting last updated dashboard.

        most_recent_dashboard = None
        for candidate in previous_version:
            current_comparator = get_dashboard(
                metabase_instance=metabase_instance, dashboard_id=candidate[2]
            )
            if most_recent_dashboard:
                current_date = datetime.datetime.fromisoformat(
                    current_comparator.get("updated_at")
                )
                current_max = datetime.datetime.fromisoformat(
                    most_recent_dashboard.get("updated_at")
                )
                current_max = (
                    current_date if current_date > current_max else current_max
                )
                continue
            most_recent_dashboard = current_comparator
        return most_recent_dashboard
    return None
