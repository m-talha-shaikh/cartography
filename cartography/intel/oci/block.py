# Copyright (c) 2020, Oracle and/or its affiliates.
# OCI Identity API-centric functions
# https://docs.cloud.oracle.com/iaas/Content/Identity/Concepts/overview.htm
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


def sync_volume_groups(
    neo4j_session: neo4j.Session,
    block: oci.core.blockstorage_client.BlockstorageClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Volume Groups for account '%s'.", current_tenancy_id)
    data = get_volume_group_list_data(block, current_tenancy_id)
    load_volume_groups(neo4j_session, data['VolumeGroups'], current_tenancy_id, oci_update_tag)
    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_volume_group_list_data(
    block: oci.core.blockstorage_client.BlockstorageClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:

    response = oci.pagination.list_call_get_all_results(block.list_volume_groups, compartment_id=current_tenancy_id)
    return {'VolumeGroups': utils.oci_object_to_json(response.data)}

def load_volume_groups(
    neo4j_session: neo4j.Session,
    volume_groups: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_volume_group = """
    MERGE(vnode:OCIVolumeGroup {ocid: $OCID})
    ON CREATE SET vnode:OCIVolumeGroup, vnode.firstseen = timestamp()
    SET vnode.displayname = $DISPLAY_NAME
    WITH vnode
    MATCH (aa:OCITenancy{ocid: $OCI_TENANCY_ID})
    MERGE (aa)-[r:RESOURCE]->(vnode)
    """

    

    for volume_group in volume_groups:

        neo4j_session.run(
            ingest_volume_group,
            OCID=volume_group["id"],
            COMPARTMENT_ID=volume_group["compartment-id"],
            # DESCRIPTION=volume_group["description"],
            DISPLAY_NAME=volume_group["display-name"],
            CREATE_DATE=volume_group["time-created"],
            OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )

def sync(
    neo4j_session: neo4j.Session,
    block: oci.core.blockstorage_client.BlockstorageClient,
    tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.info("Syncing Block Volume for account '%s'.", tenancy_id)
    sync_volume_groups(neo4j_session, block, tenancy_id, region, oci_update_tag, common_job_parameters)





