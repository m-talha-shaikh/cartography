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

def sync_subnets(
    neo4j_session: neo4j.Session,
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Subnets for account '%s'.", current_tenancy_id)
    data = get_subnet_list_data(network, current_tenancy_id)
    load_subnets(neo4j_session, data['Subnets'], current_tenancy_id, oci_update_tag)
    # run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_subnet_list_data(
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:

    response = oci.pagination.list_call_get_all_results(network.list_subnets, compartment_id=current_tenancy_id)
    return {'Subnets': utils.oci_object_to_json(response.data)}

def load_subnets(
    neo4j_session: neo4j.Session,
    subnets: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_subnet = """
    MERGE(snode:OCISubnet{ocid: $OCID})
    ON CREATE SET snode:OCISubnet, snode.firstseen = timestamp(),
    snode.createdate =  $CREATE_DATE
    SET snode.displayname = $DISPLAY_NAME
    """

    for subnet in subnets:
        neo4j_session.run(
            ingest_subnet,
            OCID=subnet["id"],
            COMPARTMENT_ID=subnet["compartment-id"],
            # DESCRIPTION=subnet["description"],
            DISPLAY_NAME=subnet["display-name"],
            CREATE_DATE=subnet["time-created"],
            # OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )

def sync_vcns(
    neo4j_session: neo4j.Session,
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing VCNS for account '%s'.", current_tenancy_id)
    data = get_vcn_list_data(network, current_tenancy_id)
    load_vcns(neo4j_session, data['VCNS'], current_tenancy_id, oci_update_tag)
    # run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_vcn_list_data(
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:

    response = oci.pagination.list_call_get_all_results(network.list_vcns, compartment_id=current_tenancy_id)
    return {'VCNS': utils.oci_object_to_json(response.data)}

def load_vcns(
    neo4j_session: neo4j.Session,
    vcns: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_vcn = """
    MERGE(snode:OCIVCN{ocid: $OCID})
    ON CREATE SET snode:OCIVCN, snode.firstseen = timestamp(),
    snode.createdate =  $CREATE_DATE
    SET snode.displayname = $DISPLAY_NAME
    """

    for vcn in vcns:
        neo4j_session.run(
            ingest_vcn,
            OCID=vcn["id"],
            COMPARTMENT_ID=vcn["compartment-id"],
            # DESCRIPTION=vcn["description"],
            DISPLAY_NAME=vcn["display-name"],
            CREATE_DATE=vcn["time-created"],
            # OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )


def sync_internet_gateways(
    neo4j_session: neo4j.Session,
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Internet Gateways for account '%s'.", current_tenancy_id)
    data = get_internet_gateway_list_data(network, current_tenancy_id)
    load_internet_gateways(neo4j_session, data['InternetGateways'], current_tenancy_id, oci_update_tag)
    # run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_internet_gateway_list_data(
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    response = oci.pagination.list_call_get_all_results(network.list_internet_gateways, compartment_id=current_tenancy_id)
    return {'InternetGateways': utils.oci_object_to_json(response.data)}

def load_internet_gateways(
    neo4j_session: neo4j.Session,
    internet_gateways: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_internet_gateway = """
    MERGE(snode:OCIInternetGateway{ocid: $OCID})
    ON CREATE SET snode:OCIInternetGateway, snode.firstseen = timestamp(),
    snode.createdate =  $CREATE_DATE
    SET snode.displayname = $DISPLAY_NAME
    """

    for internet_gateway in internet_gateways:
        neo4j_session.run(
            ingest_internet_gateway,
            OCID=internet_gateway["id"],
            COMPARTMENT_ID=internet_gateway["compartment-id"],
            # DESCRIPTION=internet_gateway["description"],
            DISPLAY_NAME=internet_gateway["display-name"],
            CREATE_DATE=internet_gateway["time-created"],
            # OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )


def sync_security_lists(
    neo4j_session: neo4j.Session,
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Security Lists for account '%s'.", current_tenancy_id)
    data = get_security_list_list_data(network, current_tenancy_id)
    load_security_lists(neo4j_session, data['SecurityLists'], current_tenancy_id, oci_update_tag)
    # run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_security_list_list_data(
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    response = oci.pagination.list_call_get_all_results(network.list_security_lists, compartment_id=current_tenancy_id)
    return {'SecurityLists': utils.oci_object_to_json(response.data)}

def load_security_lists(
    neo4j_session: neo4j.Session,
    security_lists: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_security_list = """
    MERGE(snode:OCISecurityList{ocid: $OCID})
    ON CREATE SET snode:OCISecurityList, snode.firstseen = timestamp(),
    snode.createdate =  $CREATE_DATE
    SET snode.displayname = $DISPLAY_NAME
    """

    for security_list in security_lists:
        neo4j_session.run(
            ingest_security_list,
            OCID=security_list["id"],
            COMPARTMENT_ID=security_list["compartment-id"],
            # DESCRIPTION=security_list["description"],
            DISPLAY_NAME=security_list["display-name"],
            CREATE_DATE=security_list["time-created"],
            # OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )


def sync(
    neo4j_session: neo4j.Session,
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.info("Syncing Network for account '%s'.", tenancy_id)
    sync_subnets(neo4j_session, network, tenancy_id, region, oci_update_tag, common_job_parameters)
    sync_vcns(neo4j_session, network, tenancy_id, region, oci_update_tag, common_job_parameters)
    sync_internet_gateways(neo4j_session, network, tenancy_id, region, oci_update_tag, common_job_parameters)
    sync_security_lists(neo4j_session, network, tenancy_id, region, oci_update_tag, common_job_parameters)