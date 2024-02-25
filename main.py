import sys
import argparse
import logging
import json
import time

from metabase_utils import clone_dashboard as clone


logging.basicConfig(encoding="utf-8", level=logging.INFO)


def recursive_collections_print(input_dict: dict) -> None:
    """_summary_

    Args:
        input_dict (dict): _description_
    """
    with open("blackbongo_list.json", "w", encoding="utf-8") as f:
        json.dump(input_dict, f, indent=4)


def parse_args(argv: list) -> dict:
    """_summary_

    Args:
        argv (list): _description_

    Returns:
        dict: _description_
    """
    parser = argparse.ArgumentParser(
        "SRB Automation clones source dashboards into a destination.\
              Links queries to variables and filters to cards."
    )
    parser.add_argument(
        "-cc", "--copy-collection", type=int, help="Copy entire collection from source"
    )
    parser.add_argument(
        "-nd", "--new-dashboard", help="You can specify a destination name."
    )
    parser.add_argument(
        "-sd", "--source-dashboard", help="id of dashboard in the source."
    )
    parser.add_argument(
        "-sl",
        "--source-list",
        type=int,
        nargs="+",
        help="Clone list of source dashboards. IN PROGRESS",
    )
    parser.add_argument(
        "-sad",
        "--source-available-dashboards",
        action="store_true",
        help="list of available dashboards",
    )
    parser.add_argument(
        "-pm",
        "--pm-domain",
        help="""PM domain if it\'s applicable. If the instance is in
                        the csv configuration file, this is optional.""",
    )
    parser.add_argument("-oid", "--owner-id", help="Owner ID based instance.")
    parser.add_argument("-db", "--database-name", help="Explicit database name")
    parser.add_argument("-sdb", "--source-database-name", help="Source database name")
    parser.add_argument(
        "-c", "--cache", action="store_true", help="Cache dashboards",
    )
    parser.add_argument(
        "-b",
        "--backup",
        action="store_true",
        help="Backup dashboards creating a new collection",
    )
    parser.add_argument(
        "-ent", "--enterprise", help="Use it for enterprise replication instances."
    )
    parser.add_argument(
        "-sm", "--simulate", action="store_true", help="Don't create the dashboard"
    )
    parser.add_argument(
        "-rt",
        "--root-tree",
        action="store_true",
        help="Shows info about the source instance printing the Collections tree",
    )
    parser.add_argument(
        "-gvm", "--giveme", help="Get on screen source dashboards id => name"
    )

    args = parser.parse_args(args=None if argv else ["--help"])

    return {
        "new_dashboard": args.new_dashboard,
        "source_dashboard": int(args.source_dashboard)
        if args.source_dashboard
        else None,
        "pm_domain": args.pm_domain,
        "owner_id": args.owner_id,
        "simulate": args.simulate,
        "cache": args.cache,
        "enterprise": args.enterprise,
        "giveme": args.giveme,
        "source_list": args.source_list,
        "root_tree": args.root_tree,
        "backup": args.backup,
        "copy_collection": args.copy_collection,
        "database_name": args.database_name,
        "source_database_name": args.source_database_name,
    }


def main(argv):
    logging.info("Loading script arguments")

    pretty_args = parse_args(argv)


    pm_domain_from_instance = clone.get_pm_domain_from_csv()

    # If we use Owner ID we avoid the auto matching of PM Domain Name.

    if not (
        pretty_args["source_dashboard"]
        or pretty_args["copy_collection"]
        or pretty_args["source_list"]
    ):
        raise Exception("You must specify a source dashboard id")

    source_metabase, destination_metabase = clone.setup_env()

    if pretty_args["copy_collection"]:
        clone.clone_collection_on_collection(
            source_metabase=source_metabase,
            destination_metabase=destination_metabase,
            source_collection=pretty_args["copy_collection"],
            pm_domain_name=pretty_args["pm_domain"],
            owner_id=pretty_args["owner_id"],
            enterprise=pretty_args["enterprise"],
            backup=pretty_args["backup"],
        )
        return

    if pretty_args["root_tree"]:
        list_collection = clone.list_root_collection(source_metabase)
        recursive_collections_print(list_collection)
        return


    if pretty_args["giveme"]:
        my_apology = clone.give_me_instance_dashboards_id_name(
            source_metabase, pretty_args["giveme"]
        )
        for a in my_apology:
            print(f"'{a}' : '{my_apology[a]}'")
        return

    if (
        pretty_args["pm_domain"] is None
        and pretty_args["owner_id"] is None
        and pretty_args["enterprise"] is None
    ):
        pretty_args["pm_domain"] = clone.search_pm_domain_from_instance(
            pm_domain_from_instance, destination_metabase.domain
        )
    if not (
        pretty_args["pm_domain"] or pretty_args["owner_id"] or pretty_args["enterprise"]
    ):
        raise Exception("Please indicate only one of pm-domain or owner-id")

    if pretty_args["source_list"]:
        clone.clone_multiple_dashboards(
            source_metabase=source_metabase,
            destination_metabase=destination_metabase,
            pm_domain_name=pretty_args["pm_domain"],
            owner_id=pretty_args["owner_id"],
            enterprise=pretty_args["enterprise"],
            source_list=pretty_args["source_list"],
            destination_database=pretty_args["database_name"],
        )

        return

    clone.create_and_link_dashboard(
        source_metabase=source_metabase,
        destination_metabase=destination_metabase,
        pm_domain_name=pretty_args["pm_domain"],
        owner_id=pretty_args["owner_id"],
        enterprise=pretty_args["enterprise"],
        source_dashboard=pretty_args["source_dashboard"],
        rename_dashboard=pretty_args["new_dashboard"],
        destination_database=pretty_args["database_name"],
    )


if __name__ == "__main__":
    start_time = time.time()
    main(sys.argv[1:])
    finish_time = time.time()
    print(f"Elapsed {finish_time - start_time} seconds")
