import logging
import re
from typing import Any
from typing import Dict
from typing import List

import neo4j
import oci

from . import utils
from cartography.util import run_cleanup_job

logger = logging.getLogger(__name__)


# def sync_databases(
#     neo4j_session: neo4j.Session,
#     database: oci.database.DatabaseClient,
#     current_tenancy_id: str,
#     region: str,
#     oci_update_tag: int,
#     common_job_parameters: Dict[str, Any]
# ) -> None:
#     logger.debug("Syncing Databases for account '%s'.", current_tenancy_id)
#     data = get_database_list_data(database, current_tenancy_id)
#     # load_databases(neo4j_session, data['Databases'], current_tenancy_id, oci_update_tag)

#     # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


# def get_database_list_data(
#     database: oci.database.DatabaseClient,
#     current_tenancy_id: str,
#     db_home_id: str = None,
#     system_id: str = None
# ) -> Dict[str, List[Dict[str, Any]]]:

#     # Prepare optional parameters
#     optional_params = {}
#     if db_home_id:
#         optional_params['db_home_id'] = db_home_id
#     if system_id:
#         optional_params['system_id'] = system_id

#     # Call list_databases with all parameters
#     response = oci.pagination.list_call_get_all_results(
#         database.list_databases,
#         compartment_id=current_tenancy_id,
#         **optional_params
#     )
#     return {'Databases': utils.oci_object_to_json(response.data)}


# def load_databases(
#     neo4j_session: neo4j.Session,
#     databases: List[Dict[str, Any]],
#     current_oci_tenancy_id: str,
#     oci_update_tag: int,
# ) -> None:
#     ingest_database = """
#     MERGE(inode:OCIDatabase{ocid: $OCID})
#     ON CREATE SET inode:OCIDatabase, inode.firstseen = timestamp(),
#     inode.createdate =  $CREATE_DATE
#     SET inode.displayname = $DISPLAY_NAME
#     """


#     for database in databases:
#         neo4j_session.run(
#             ingest_database,
#             OCID=database["id"],
#             COMPARTMENT_ID=database["compartment-id"],
#             # DESCRIPTION=volume_attachment["description"],
#             CREATE_DATE=database["time-created"],
#             # OCI_TENANCY_ID=current_oci_tenancy_id,
#             oci_update_tag=oci_update_tag,
#         )


def sync_autonomous_databases(
    neo4j_session: neo4j.Session,
    database: oci.database.DatabaseClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Databases for account '%s'.", current_tenancy_id)
    data = get_autonomous_database_list_data(database, current_tenancy_id)
    load_autonomous_databases(neo4j_session, data['Databases'], current_tenancy_id, oci_update_tag)

    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_autonomous_database_list_data(
    database: oci.database.DatabaseClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:

    response = oci.pagination.list_call_get_all_results(database.list_autonomous_databases, compartment_id=current_tenancy_id)
    return {'Databases': utils.oci_object_to_json(response.data)}

def load_autonomous_databases(
    neo4j_session: neo4j.Session,
    autonomous_databases: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_autonomous_database = """
    MERGE(inode:OCIDatabase{ocid: $OCID})
    ON CREATE SET inode:OCIDatabase, inode.firstseen = timestamp(),
    inode.createdate =  $CREATE_DATE
    SET inode.oci_update_tag = $oci_update_tag
    WITH inode
    MATCH (aa:OCITenancy{ocid: $OCI_TENANCY_ID})
    MERGE (aa)-[r:RESOURCE]->(inode)
    """


    for autonomous_database in autonomous_databases:
        neo4j_session.run(
            ingest_autonomous_database,
            OCID=autonomous_database["id"],
            COMPARTMENT_ID=autonomous_database["compartment-id"],
            # DESCRIPTION=volume_attachment["description"],
            CREATE_DATE=autonomous_database["time-created"],
            OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )

def sync(
    neo4j_session: neo4j.Session,
    database: oci.database.DatabaseClient,
    tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.info("Syncing Database for account '%s'.", tenancy_id)
    sync_autonomous_databases(neo4j_session, database, tenancy_id, region, oci_update_tag, common_job_parameters)