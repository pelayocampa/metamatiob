from __future__ import annotations

import arrow
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from dataclasses_json import config, dataclass_json
from marshmallow import fields
from metabase_api import Metabase_API

class ArrowEncoderDecoder():
    @staticmethod
    def encode(x: arrow.Arrow) -> str:
        return x.isoformat()

    @staticmethod
    def decode(x: str) -> arrow.Arrow:
        return arrow.get(x)

@dataclass_json
@dataclass
class Field:
    id: int
    name: str
    display_name: str
    base_type: str

    def to_dict(self) -> dict:
        node_dict = {
            "id": self.id,
            "name": self.name,
            "display_name": self.display_name,
            "base_type": self.base_type,
        }

        return node_dict

    def __eq__(self, other: Field) -> bool:
        """That way we can match different fields from different databases

        Args:
            other (Field): Another Field to compare.

        Returns:
            bool: True if name field is equal in both objects.
        """
        return True if self.name == other.name else False

    def __lt__(self, other: Field) -> bool:
        return True if self.name < other.name else False


@dataclass_json
@dataclass
class Database:
    name: str
    id: int
    engine: str
    tables: List[Table]

    def search_table(self, pattern: str = "") -> Table:
        compiled_pattern = re.compile(r"{pattern}")
        for element in self.tables:
            if re.search(compiled_pattern, element["name"]):
                return element
        raise KeyError(f"Could not find field matching {pattern}")

    def search_field(self, pattern: str = "") -> dict:
        field_dict: dict() = {}
        for element in self.tables:
            field_dict[element["id"]] = element.search_field(pattern=pattern)

        return field_dict

    def to_dict(self) -> dict:
        node_dict = {
            "id": self.id,
            "name": self.name,
            "tables": self.tables.to_dict() if self.tables else None,
        }

        return node_dict


@dataclass_json
@dataclass
class Table:
    id: int = None
    schema: str = None
    name: str = None
    display_name: str = None
    fields: List[Field] = None

    def __post_init__(self):
        self.__hashfunction = hashlib.sha256()

    def search_field(self, pattern: str = "") -> Field:
        compiled_pattern = re.compile(r"{pattern}")
        for element in self.fields:
            if re.search(compiled_pattern, element["name"]):
                return element
        raise KeyError(f"Could not find field matching {pattern}")

    def __eq__(self, other: Table) -> bool:
        """Checks table names. First attempt to match them without using
        hash

        Args:
            other (Table): Another Table instance.

        Raises:
            ValueError: If the other parameter isn't a Table instance.

        Returns:
            bool: _description_
        """
        if isinstance(other, Table):
            return True if self.name == other.name else False
        raise ValueError("Not the same class")

    def to_dict(self) -> dict:
        node_dict = {
            "id": self.id,
            "schema": self.schema,
            "name": self.name,
            "display_name": self.display_name,
            "fields": self.fields.to_dict() if self.fields else None,
        }
        return node_dict

    def __hash__(self) -> int:
        """Generate a hash code for this table. Uses the names of the fields
        list to determine if two tables are similar. That's, having different names
        have the same fileds name.

        Returns:
            int: Hash code
        """
        self.fields.sort()
        hashit = tuple(x.name for x in self.fields)
        self.__hashfunction.update(str(hashit).encode())
        return int(self.__hashfunction.hexdigest(), 16)


@dataclass_json
@dataclass
class Collection:
    id: int
    description: Optional[str] = None
    children: List[Collection] = None
    slug: str = None
    color: str = None
    name: str = None
    personal_owner_id: Optional[int] = None
    location: str = None
    namespace: Optional[str] = None


@dataclass_json
@dataclass
class Dashboard:
    id: int
    updated_at: datetime = field(
        metadata=config(
            encoder=ArrowEncoderDecoder.encode,
            decoder=ArrowEncoderDecoder.decode,
            mm_field=fields.DateTime(format="iso"),
        )
    )
    collection_id: Optional[int] = None
    name: str = None

    def __eq__(self, other):
        if isinstance(other, str):
            if not other.isdigit():
                return self.name == other
            return self.id == int(other)
        return self.id == other


class MetabaseData:
    """This clases represents a basic Metabase Instance.
    Stores databases, collections and references to dashboards.

    Inherit from it as base class for DMO and SRB instances.
    """

    def __init__(
        self,
        metabase_instance: Metabase_API = None,
        email: str = None,
        password: str = None,
        domain: str = None,
    ) -> None:
        if email and password and domain:
            metabase_instance = Metabase_API(
                email=email, password=password, domain=domain
            )
        self.__cached = True
        self.__config_folder = os.getenv("CONFIG_FOLDER") or "config"
        self.__metabase_instance = metabase_instance
        self.__parse_instance()
        self.__setup_cache()
        self.__populate_databases()
        self.__populate_dashboards()
        self.__populate_collections()

    @property
    def instance_name(self) -> str:
        if self.__instance_name is None:
            raise ValueError("Instance not active")
        return self.__instance_name

    @property
    def cached(self) -> bool:
        return self.__cached

    @property
    def databases(self) -> List[Database]:
        if self.__databases:
            return self.__databases
        raise NameError("No databases defined")

    @property
    def collections(self) -> List[Collection]:
        if self.__collections:
            return self.__collections
        raise ValueError("No collections available")

    @property
    def dashboards(self) -> List[Dashboard]:
        if self.__dashboards:
            return self.__dashboards
        raise ValueError("No dashboards available")

    def __populate_databases(self) -> None:
        """Relies on cache system to deserialize database info into a custom data_structure"""
        request = self.__metabase_instance.get("/api/database")["data"]

        for database in request:
            database_element: dict
            search_path = f"{self.__config_folder}/{self.__instance_name}/database/{database['id']}.json"

            file_exists = os.path.exists(search_path)
            if file_exists:
                with open(search_path, "r", encoding="utf8") as f:
                    database_element = json.load(f)

            # if not file_exists or datetime.fromisoformat(
            # database_element["updated_at"]
            # ) < datetime.fromisoformat(database["updated_at"]):
            if not file_exists or arrow.get(
                database_element["updated_at"]
            ) < arrow.get(database["updated_at"]):
                database_element = self.__metabase_instance.get(
                    f"/api/database/{database['id']}?include=tables.fields"
                )
                self.cache(path=search_path, data=database_element)

            try:
                _ = self.databases
            except Exception:
                self.__databases = []
            finally:
                self.__databases.append(Database.from_dict(database_element))

    def __populate_dashboards(self) -> None:
        """Populates a list of dashboards on the instance."""
        request = self.__metabase_instance.get("/api/dashboard")
        self.__dashboards = [Dashboard.from_dict(x) for x in request]

    def __populate_collections(self) -> None:
        request = self.__metabase_instance.get("/api/collection/tree")
        self.__collections = [Collection.from_dict(x) for x in request]

    def __parse_instance(self) -> None:
        """Sometimes we need to use the instance name to make some comparisons"""
        compiled_pattern = re.compile(r"https?://(\w+)\.seetransparent\.com")
        matched = re.search(compiled_pattern, self.__metabase_instance.domain)
        self.__instance_name = matched.group(1)

    def get_database_id(self, name: str) -> int:
        pass

    def dashboard(self, id: int = None) -> dict:
        current_dashboard = {}
        element = self.dashboards[self.dashboards.index(id)]
        search_path = f"{self.__config_folder}/{self.__instance_name}/dashboard/{id}.json"
        
        file_exists = os.path.exists(search_path)
        if file_exists:
            with open(search_path, "r", encoding="utf8") as f:
                current_dashboard = json.load(f)

        if (
            not file_exists
            or datetime.fromisoformat(current_dashboard["updated_at"])
            < element.updated_at
        ):
            self.__cached = False
            current_dashboard = self.__metabase_instance.get(
                f"/api/dashboard/{id}"
            )
            self.cache(path=search_path, data=current_dashboard)

        return current_dashboard

    def cache(self, path: str, data: dict) -> None:
        """Saves a dict

        Args:
            path (str): _description_
            data (dict): _description_

        """
        with open(path, "w", encoding="utf8") as f:
            json.dump(data, f, indent=4)

    def __setup_cache(self):
        subfolders = ["dashboard", "database"]
        base_path = f"{self.__config_folder}/{self.__instance_name}/"

        _ = [
            os.makedirs(name=base_path + x, exist_ok=True) for x in subfolders
        ]

    def __str__(self) -> str:
        return str(self.__databases)

    def to_dict(self) -> dict:
        if self.__databases:
            return [x.to_dict() for x in self.__databases]
        raise ValueError("No databases defined")

    def __eq__(self, other) -> bool:
        if isinstance(other, MetabaseData):
            return other.instance_name == self.instance_name
        raise ValueError("Instances aren't MetabaseData type")


class MetabaseSRBData(MetabaseData):
  
    def __init__(
        self,
        metabase_instance: Metabase_API = None,
        email: str = None,
        password: str = None,
        domain: str = None,
        pm_domain_name: str = None,
        owner_id: str = None,
    ):
        self.__pm_domain_name = pm_domain_name
        self.owner_id = owner_id
        super().__init__(
            metabase_instance=metabase_instance,
            email=email,
            password=password,
            domain=domain,
        )


class FieldMatcher(object):
    def __init__(
        self,
        data_source: MetabaseData = None,
        data_destination: MetabaseData = None,
        source_instance: Metabase_API = None,
        destination_instance: Metabase_API = None,
    ):
        if source_instance:
            data_source = MetabaseData(source_instance)
        if destination_instance:
            data_destination = MetabaseData(destination_instance)
        self.__config_folder = os.getenv("CONFIG_FOLDER")
        self.__databases_tables_fields_match = None
        self.__data_source = data_source
        self.__data_destination = data_destination
        self.__same_instance = data_source == data_destination

    @property
    def matches(self) -> dict:
        return (
            self.__databases_tables_fields_match
            if self.__databases_tables_fields_match
            else self.match()
        )

    def as_integer(self, x: dict) -> dict:
        return {int(k): v for k, v in x.items()}

    def match(self) -> dict:
        folder = f"{self.__config_folder}/{self.__data_source.instance_name}/{self.__data_destination.instance_name}"
        os.makedirs(folder, exist_ok=True)

        file_exists = os.path.isfile(f"{folder}/field_match.json")
        match_dict = {}
        if (
            self.__data_source.cache
            and self.__data_destination.cache
            and file_exists
        ):
            with open(
                f"{folder}/field_match.json", "r", encoding="UTF-8"
            ) as f:
                match_dict = json.load(f, object_hook=self.as_integer)
                # Using an object hook.

        else:
            for source_element in self.__data_source.databases:
                for destination_element in self.__data_destination.databases:
                    if (
                        self.__same_instance
                        and (source_element.id == destination_element.id)
                        or source_element.engine != destination_element.engine
                    ):
                        continue
                    if match_dict.get(source_element.id) is None:
                        match_dict[source_element.id] = {}

                    match_dict[source_element.id][destination_element.id] = (
                        self.match_databases(
                            source_element, destination_element
                        )
                    )

                    with open(
                        f"{folder}/field_match.json", "w", encoding="UTF-8"
                    ) as f:
                        json.dump(match_dict, f, indent=4)

        self.__databases_tables_fields_match = match_dict
        return match_dict

    def match_tables(
        self, source_table: Table, destination_table: Table
    ) -> dict:
        matching_fields = {}
        for source_field in source_table.fields:
            for destination_field in destination_table.fields:
                if source_field.name == destination_field.name:
                    matching_fields[source_field.id] = destination_field.id
                    break
        return matching_fields

    def match_databases(
        self, source_database: Database, destination_database: Database
    ) -> dict:
        matching_tables = {}
        for source_table in source_database.tables:
            for destination_table in destination_database.tables:
                if source_table.name == destination_table.name or hash(
                    source_table
                ) == hash(destination_table):
                    matching_tables = {
                        **matching_tables,
                        **self.match_tables(source_table, destination_table),
                    }
                    break
        return matching_tables
