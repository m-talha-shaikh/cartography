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

def sync_loadBalancers(
    neo4j_session: neo4j.Session,
    loadBalancer: oci.load_balancer.LoadBalancerClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Load Balancers for account '%s'.", current_tenancy_id)
    data = get_load_balancer_list_data(loadBalancer, current_tenancy_id)

    # load_load_balancers(neo4j_session, data['LoadBalancers'], current_tenancy_id, oci_update_tag)
    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_load_balancer_list_data(
    loadBalancer: oci.load_balancer.LoadBalancerClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    response = oci.pagination.list_call_get_all_results(loadBalancer.list_load_balancers, compartment_id=current_tenancy_id)
     
    return {'LoadBalancers': utils.oci_object_to_json(response.data)}

def load_load_balancers(
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


def sync(
    neo4j_session: neo4j.Session,
    loadBalancer: oci.load_balancer.LoadBalancerClient,
    tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.info("Syncing Load Balancer for account '%s'.", tenancy_id)
    sync_loadBalancers(neo4j_session, loadBalancer, tenancy_id, region, oci_update_tag, common_job_parameters)