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

def sync_gateways(
    neo4j_session: neo4j.Session,
    gateway: oci.apigateway.GatewayClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    
    logger.debug("Syncing Gateways for account '%s'.", current_tenancy_id)
    data = get_gateway_list_data(gateway, current_tenancy_id)

    load_gateways(neo4j_session, data['Gateways'], current_tenancy_id, oci_update_tag)
    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_gateway_list_data(
    gateway: oci.apigateway.GatewayClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    response = oci.pagination.list_call_get_all_results(gateway.list_gateways, compartment_id=current_tenancy_id)
     
    return {'Gateways' : utils.oci_object_to_json(response.data)}

def load_gateways(
    neo4j_session: neo4j.Session,
    gateways: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_gateway = """
        MERGE (gnode:OCIGateway {ocid: $OCID})
        ON CREATE SET gnode:OCIGateway,
                    gnode.firstseen = timestamp(),
                    gnode.lifecycle_state = $LIFECYCLE_STATE,
                    gnode.endpoint_type = $ENDPOINT_TYPE,
                    gnode.hostname = $HOSTNAME,
                    gnode.lifecycle_details = $LIFECYCLE_DETAILS,
                    gnode.network_security_group_ids = $NETWORK_SECURITY_GROUP_IDS,
                    gnode.subnet_id = $SUBNET_ID,
                    gnode.time_created = $TIME_CREATED,
                    gnode.time_updated = $TIME_UPDATED
        SET gnode.displayname = $DISPLAY_NAME,
            gnode.lastupdated = timestamp()
        WITH gnode
        MATCH (aa:OCITenancy {ocid: $OCI_TENANCY_ID})
        MERGE (aa)-[r:RESOURCE]->(gnode)
        """

    for gateway in gateways:
        neo4j_session.run(
            ingest_gateway,
            OCID=gateway["id"],
            LIFECYCLE_STATE=gateway["lifecycle-state"],
            ENDPOINT_TYPE=gateway["endpoint-type"],
            HOSTNAME=gateway["hostname"],
            LIFECYCLE_DETAILS=gateway["lifecycle-details"],
            NETWORK_SECURITY_GROUP_IDS=gateway["network-security-group-ids"],
            SUBNET_ID=gateway["subnet-id"],
            TIME_CREATED=gateway["time-created"],
            TIME_UPDATED=gateway["time-updated"],
            DISPLAY_NAME=gateway["display-name"],
            OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )

def sync(
    neo4j_session: neo4j.Session,
    gateway: oci.apigateway.GatewayClient,
    tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.info("Syncing Network for account '%s'.", tenancy_id)
    sync_gateways(neo4j_session, gateway, tenancy_id, region, oci_update_tag, common_job_parameters)