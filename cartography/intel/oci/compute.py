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


def sync_instances(
    neo4j_session: neo4j.Session,
    compute: oci.core.compute_client.ComputeClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Instances for account '%s'.", current_tenancy_id)
    data = get_instance_list_data(compute, current_tenancy_id)
    # load_instances(neo4j_session, data['Instances'], current_tenancy_id, oci_update_tag)
    # run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_instance_list_data(
    compute: oci.core.compute_client.ComputeClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:

    response = oci.pagination.list_call_get_all_results(compute.list_instances, compartment_id=current_tenancy_id)
    return {'Instances': utils.oci_object_to_json(response.data)}

def load_instances(
    neo4j_session: neo4j.Session,
    instances: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_instance = """
    MERGE(inode:OCIInstances{ocid: $OCID})
    ON CREATE SET inode:OCIInstance, inode.firstseen = timestamp(),
    inode.createdate =  $CREATE_DATE
    SET inode.displayname = $DISPLAY_NAME
    """


    for instance in instances:
        neo4j_session.run(
            ingest_instance,
            OCID=instance["id"],
            COMPARTMENT_ID=instance["compartment-id"],
            # DESCRIPTION=volume_attachment["description"],
            DISPLAY_NAME=instance["display-name"],
            CREATE_DATE=instance["time-created"],
            # OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )


def sync_volume_attachments(
    neo4j_session: neo4j.Session,
    compute: oci.core.compute_client.ComputeClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Volume Attachments for account '%s'.", current_tenancy_id)
    data = get_volume_attachment_list_data(compute, current_tenancy_id)
    load_volume_attachments(neo4j_session, data['VolumeAttachments'], current_tenancy_id, oci_update_tag)
    # run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)

def load_volume_attachments(
    neo4j_session: neo4j.Session,
    volume_attachments: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_volume_attachment = """
    MERGE(vnode:OCIVolumeAttachment{ocid: $OCID})
    ON CREATE SET vnode:OCIVolumeAttachment, vnode.firstseen = timestamp(),
    vnode.createdate =  $CREATE_DATE
    SET vnode.displayname = $DISPLAY_NAME
    """


    for volume_attachment in volume_attachments:
        neo4j_session.run(
            ingest_volume_attachment,
            OCID=volume_attachment["id"],
            COMPARTMENT_ID=volume_attachment["compartment-id"],
            # DESCRIPTION=volume_attachment["description"],
            DISPLAY_NAME=volume_attachment["display-name"],
            CREATE_DATE=volume_attachment["time-created"],
            # OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )

    # ingest_compartment = """
    # MERGE (cnode:OCICompartment{ocid: $OCID})
    # ON CREATE SET cnode:OCICompartment, cnode.firstseen = timestamp(),
    # cnode.createdate = $CREATE_DATE
    # SET cnode.name = $NAME, cnode.compartmentid = $COMPARTMENT_ID
    # WITH cnode
    # MATCH (aa) WHERE (aa:OCITenancy OR aa:OCICompartment) AND aa.ocid=$COMPARTMENT_ID
    # MERGE (aa)-[r:OCI_COMPARTMENT]->(cnode)
    # ON CREATE SET r.firstseen = timestamp()
    # SET r.lastupdated = $oci_update_tag
    # """

    # for compartment in compartments:
    #     neo4j_session.run(
    #         ingest_compartment,
    #         OCID=compartment["id"],
    #         COMPARTMENT_ID=compartment["compartment-id"],
    #         DESCRIPTION=compartment["description"],
    #         NAME=compartment["name"],
    #         CREATE_DATE=compartment["time-created"],
    #         OCI_TENANCY_ID=current_oci_tenancy_id,
    #         oci_update_tag=oci_update_tag,
    #     )


def get_volume_attachment_list_data(
    compute: oci.core.compute_client.ComputeClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:

    response = oci.pagination.list_call_get_all_results(compute.list_volume_attachments, compartment_id=current_tenancy_id)
    return {'VolumeAttachments': utils.oci_object_to_json(response.data)}


def sync_vnic_attachments(
    neo4j_session: neo4j.Session,
    compute: oci.core.compute_client.ComputeClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Vnic Attachments for account '%s'.", current_tenancy_id)
    data = get_vnic_attachment_list_data(compute, current_tenancy_id)
    # load_vnic_attachments(neo4j_session, data['VnicAttachments'], current_tenancy_id, oci_update_tag)
    # run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_vnic_attachment_list_data(
    compute: oci.core.compute_client.ComputeClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:

    response = oci.pagination.list_call_get_all_results(compute.list_vnic_attachments, compartment_id=current_tenancy_id)
    return {'VnicAttachments': utils.oci_object_to_json(response.data)}


def sync_compute_clusters(
    neo4j_session: neo4j.Session,
    compute: oci.core.compute_client.ComputeClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.debug("Syncing Compute Clusters for account '%s'.", current_tenancy_id)
    data = get_compute_cluster_list_data(compute, current_tenancy_id)
    # load_compute_clusters(neo4j_session, data['ComputeClusters'], current_tenancy_id, oci_update_tag)
    # run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_compute_cluster_list_data(
    compute: oci.core.compute_client.ComputeClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:

    response = oci.pagination.list_call_get_all_results(compute.list_compute_clusters, compartment_id=current_tenancy_id)
    return {'ComputeClusters': utils.oci_object_to_json(response.data)}

def sync(
    neo4j_session: neo4j.Session,
    compute: oci.core.compute_client.ComputeClient,
    tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:
    logger.info("Syncing Compute for account '%s'.", tenancy_id)
    sync_instances(neo4j_session, compute, tenancy_id, region, oci_update_tag, common_job_parameters)
    sync_volume_attachments(neo4j_session, compute, tenancy_id, region, oci_update_tag, common_job_parameters)
    sync_vnic_attachments(neo4j_session, compute, tenancy_id, region, oci_update_tag, common_job_parameters)
    sync_compute_clusters(neo4j_session, compute, tenancy_id, region, oci_update_tag, common_job_parameters)