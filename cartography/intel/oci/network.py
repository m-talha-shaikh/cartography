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


SUBNET_VCN = []
SECURITY_GROUP_VCN = []

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

    for subnet in data['Subnets']:
        SUBNET_VCN.append((subnet['id'], subnet['vcn-id']))

    load_subnets(neo4j_session, data['Subnets'], current_tenancy_id, oci_update_tag)
    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


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
        MERGE (snode:OCISubnet {ocid: $OCID})
        ON CREATE SET snode:OCISubnet,
                    snode.firstseen = timestamp(),
                    snode.createdate = $CREATE_DATE,
                    snode.cidr_block = $CIDR_BLOCK,
                    snode.lifecycle_state = $LIFECYCLE_STATE,
                    snode.prohibit_internet_ingress = $PROHIBIT_INTERNET_INGRESS,
                    snode.prohibit_public_ip_on_vnic = $PROHIBIT_PUBLIC_IP_ON_VNIC,
                    snode.route_table_id = $ROUTE_TABLE_ID,
                    snode.subnet_domain_name = $SUBNET_DOMAIN_NAME,
                    snode.virtual_router_ip = $VIRTUAL_ROUTER_IP,
                    snode.virtual_router_mac = $VIRTUAL_ROUTER_MAC,
                    snode.dhcp_options_id = $DHCP_OPTIONS_ID,
                    snode.dns_label = $DNS_LABEL,
                    snode.vcn_id = $VCN_ID,
                    snode.security_list_ids = $SECURITY_LIST_IDS,
                    snode.ipv6_cidr_block = $IPV6_CIDR_BLOCK,
                    snode.ipv6_cidr_blocks = $IPV6_CIDR_BLOCKS,
                    snode.ipv6_virtual_router_ip = $IPV6_VIRTUAL_ROUTER_IP
        SET snode.displayname = $DISPLAY_NAME,
            snode.lastupdated = timestamp()
        WITH snode
        MATCH (aa:OCITenancy {ocid: $OCI_TENANCY_ID})
        MERGE (aa)-[r:RESOURCE]->(snode)
        """

    for subnet in subnets:
        neo4j_session.run(
            ingest_subnet,
            OCID=subnet["id"],
            CIDR_BLOCK=subnet["cidr-block"],
            LIFECYCLE_STATE=subnet["lifecycle-state"],
            PROHIBIT_INTERNET_INGRESS=subnet["prohibit-internet-ingress"],
            PROHIBIT_PUBLIC_IP_ON_VNIC=subnet["prohibit-public-ip-on-vnic"],
            ROUTE_TABLE_ID=subnet["route-table-id"],
            SUBNET_DOMAIN_NAME=subnet["subnet-domain-name"],
            VIRTUAL_ROUTER_IP=subnet["virtual-router-ip"],
            VIRTUAL_ROUTER_MAC=subnet["virtual-router-mac"],
            DHCP_OPTIONS_ID=subnet["dhcp-options-id"],
            DNS_LABEL=subnet["dns-label"],
            VCN_ID=subnet["vcn-id"],
            SECURITY_LIST_IDS=subnet["security-list-ids"],
            IPV6_CIDR_BLOCK=subnet["ipv6-cidr-block"],
            IPV6_CIDR_BLOCKS=subnet["ipv6-cidr-blocks"],
            IPV6_VIRTUAL_ROUTER_IP=subnet["ipv6-virtual-router-ip"],
            DISPLAY_NAME=subnet["display-name"],
            CREATE_DATE=subnet["time-created"],
            OCI_TENANCY_ID=current_oci_tenancy_id,
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
    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


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
    SET snode.displayname = $DISPLAY_NAME,
        snode.cidr_block = $CIDR_BLOCK,
        snode.default_dhcp_options_id = $DEFAULT_DHCP_OPTIONS_ID,
        snode.default_route_table_id = $DEFAULT_ROUTE_TABLE_ID,
        snode.default_security_list_id = $DEFAULT_SECURITY_LIST_ID,
        snode.lifecycle_state = $LIFECYCLE_STATE,
        snode.time_created = $TIME_CREATED,
        snode.vcn_domain_name = $VCN_DOMAIN_NAME
    WITH snode
    MATCH (aa:OCITenancy{ocid: $OCI_TENANCY_ID})
    MERGE (aa)-[r:RESOURCE]->(snode)
    """

    for vcn in vcns:
        neo4j_session.run(
            ingest_vcn,
            OCID=vcn["id"],
            COMPARTMENT_ID=vcn["compartment-id"],
            DISPLAY_NAME=vcn["display-name"],
            CREATE_DATE=vcn["time-created"],
            OCI_TENANCY_ID=current_oci_tenancy_id,
            CIDR_BLOCK=vcn["cidr-block"],
            DEFAULT_DHCP_OPTIONS_ID=vcn["default-dhcp-options-id"],
            DEFAULT_ROUTE_TABLE_ID=vcn["default-route-table-id"],
            DEFAULT_SECURITY_LIST_ID=vcn["default-security-list-id"],
            LIFECYCLE_STATE=vcn["lifecycle-state"],
            TIME_CREATED=vcn["time-created"],
            VCN_DOMAIN_NAME=vcn["vcn-domain-name"],
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
    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


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
    WITH snode
    MATCH (aa:OCITenancy{ocid: $OCI_TENANCY_ID})
    MERGE (aa)-[r:RESOURCE]->(snode)    
    """

    for internet_gateway in internet_gateways:
        neo4j_session.run(
            ingest_internet_gateway,
            OCID=internet_gateway["id"],
            COMPARTMENT_ID=internet_gateway["compartment-id"],
            # DESCRIPTION=internet_gateway["description"],
            DISPLAY_NAME=internet_gateway["display-name"],
            CREATE_DATE=internet_gateway["time-created"],
            OCI_TENANCY_ID=current_oci_tenancy_id,
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
    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


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
    WITH snode
    MATCH (aa:OCITenancy{ocid: $OCI_TENANCY_ID})
    MERGE (aa)-[r:RESOURCE]->(snode)
    """

    for security_list in security_lists:
        neo4j_session.run(
            ingest_security_list,
            OCID=security_list["id"],
            COMPARTMENT_ID=security_list["compartment-id"],
            # DESCRIPTION=security_list["description"],
            DISPLAY_NAME=security_list["display-name"],
            CREATE_DATE=security_list["time-created"],
            OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )

def sync_network_security_groups(
    neo4j_session: neo4j.Session,
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Network Security Groups for account '%s'.", current_tenancy_id)
    data = get_network_security_group_list_data(network, current_tenancy_id)

    for security_group in data['NetworkSecurityGroups']:
      SECURITY_GROUP_VCN.append((security_group['id'], security_group['vcn-id']))

    load_network_security_groups(neo4j_session, data['NetworkSecurityGroups'], current_tenancy_id, oci_update_tag)
    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_network_security_group_list_data(
    network: oci.core.virtual_network_client.VirtualNetworkClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    response = oci.pagination.list_call_get_all_results(network.list_network_security_groups, compartment_id=current_tenancy_id)
    return {'NetworkSecurityGroups': utils.oci_object_to_json(response.data)}

def load_network_security_groups(
    neo4j_session: neo4j.Session,
    network_security_groups: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_network_security_group = """
    MERGE(snode:OCINetworkSecurityGroup{ocid: $OCID})
    ON CREATE SET snode:OCINetworkSecurityGroup, snode.firstseen = timestamp(),
    snode.createdate =  $CREATE_DATE
    SET snode.displayname = $DISPLAY_NAME
    WITH snode
    MATCH (aa:OCITenancy{ocid: $OCI_TENANCY_ID})
    MERGE (aa)-[r:RESOURCE]->(snode)
    """

    for network_security_group in network_security_groups:
        neo4j_session.run(
            ingest_network_security_group,
            OCID=network_security_group["id"],
            COMPARTMENT_ID=network_security_group["compartment-id"],
            # DESCRIPTION=network_security_group["description"],
            DISPLAY_NAME=network_security_group["display-name"],
            CREATE_DATE=network_security_group["time-created"],
            OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )


def sync_network_references(
    neo4j_session: neo4j.Session,
    tenancy_id: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any],
) -> None:
    for subnet_data in SUBNET_VCN:
        load_oci_network_subnet_reference(neo4j_session, subnet_data[0], subnet_data[1], tenancy_id, oci_update_tag)

def load_oci_network_subnet_reference(
    neo4j_session: neo4j.Session,
    subnet_id: str,
    vcn_id: str,
    tenancy_id: str,
    oci_update_tag: int,
) -> None:

    ingest_policy_group_reference = """
    MERGE (aa:OCISubnet {ocid: $SUBNET_ID})
    MERGE (bb:OCIVCN {ocid: $VCN_ID})
    MERGE (aa)-[r:MEMBER_OF_VCN]->(bb)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $oci_update_tag
    """
    neo4j_session.run(
        ingest_policy_group_reference,
        SUBNET_ID=subnet_id,
        VCN_ID=vcn_id,
        oci_update_tag=oci_update_tag,
    )

def sync_network_security_group_references(
    neo4j_session: neo4j.Session,
    tenancy_id: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any],
) -> None:
    for network_security_group in SECURITY_GROUP_VCN:
        load_oci_network_security_group_vcn_reference(neo4j_session, network_security_group[0], network_security_group[1], tenancy_id, oci_update_tag)

def load_oci_network_security_group_vcn_reference(
    neo4j_session: neo4j.Session,
    network_security_group_id: str,
    vcn_id: str,
    tenancy_id: str,
    oci_update_tag: int,
) -> None:

    ingest_policy_group_reference = """
    MERGE (aa:OCINetworkSecurityGroup {ocid: $NETWORK_SECURITY_GROUP_ID})
    MERGE (bb:OCIVCN {ocid: $VCN_ID})
    MERGE (bb)-[r:MEMBER_OF_NETWORK_SECURITY_GROUP]->(aa)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $oci_update_tag
    """
    neo4j_session.run(
        ingest_policy_group_reference,
        NETWORK_SECURITY_GROUP_ID=network_security_group_id,
        VCN_ID=vcn_id,
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
    sync_network_security_groups(neo4j_session, network, tenancy_id, region, oci_update_tag, common_job_parameters)
    sync_network_references(neo4j_session, region, oci_update_tag, common_job_parameters)
    sync_network_security_group_references(neo4j_session, region, oci_update_tag, common_job_parameters)
    