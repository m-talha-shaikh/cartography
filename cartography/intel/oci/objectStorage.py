import logging
import re
from typing import Any
from typing import Dict
from typing import List

import neo4j
import oci

from . import utils
from cartography.util import run_cleanup_job

buckets = []

from . import iam

logger = logging.getLogger(__name__)

REGIONS = {}

def sync_buckets(
    neo4j_session: neo4j.Session,
    objectStorage: oci.object_storage.ObjectStorageClient,
    current_tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any]
) -> None:

    logger.debug("Syncing Buckets for account '%s'.", current_tenancy_id)
    data = get_bucket_list_data(objectStorage, current_tenancy_id)
    namespace = objectStorage.get_namespace().data

    for bucket in data['Buckets']:
        bucketResponse = objectStorage.get_bucket(namespace_name=namespace, bucket_name=bucket['name'])
        buckets.append(bucketResponse.data)

    final_buckets = utils.oci_object_to_json(buckets)

    load_buckets(neo4j_session, final_buckets, current_tenancy_id, oci_update_tag)
    # #run_cleanup_job('oci_import_users_cleanup.json', neo4j_session, common_job_parameters)


def get_bucket_list_data(
    objectStorage: oci.object_storage.ObjectStorageClient,
    current_tenancy_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    
    namespace = objectStorage.get_namespace().data

    response = oci.pagination.list_call_get_all_results(objectStorage.list_buckets, namespace_name=namespace, compartment_id=current_tenancy_id)

    return {'Buckets': utils.oci_object_to_json(response.data)}

def load_buckets(
    neo4j_session: neo4j.Session,
    buckets: List[Dict[str, Any]],
    current_oci_tenancy_id: str,
    oci_update_tag: int,
) -> None:
    ingest_bucket = """
    MERGE(inode:OCIBucket{ocid: $OCID})
    ON CREATE SET inode:OCIBucket, inode.firstseen = timestamp(),
    inode.createdate =  $CREATE_DATE, inode.tags = $TAGS
    SET inode.name = $NAME
    WITH inode
    MATCH (aa:OCITenancy{ocid: $OCI_TENANCY_ID})
    MERGE (aa)-[r:RESOURCE]->(inode)
    """


    for bucket in buckets:
        tag_list = utils.extract_namespace_tags(bucket["defined-tags"])
        tag_list2 = utils.extract_namespace_tags2(bucket["defined-tags"])
        # print(type(tag_list2))
        # print(tag_list2)

        neo4j_session.run(
            ingest_bucket,
            OCID=bucket["id"],
            ETAG=bucket["etag"],
            COMPARTMENT_ID=bucket["compartment-id"],
            TAGS=str(tag_list),
            NAME=bucket["name"],
            CREATE_DATE=bucket["time-created"],
            CREATED_BY=bucket["defined-tags"]["Oracle-Tags"]["CreatedBy"],
            OCI_TENANCY_ID=current_oci_tenancy_id,
            oci_update_tag=oci_update_tag,
        )

        #tags = utils.extract_key_value_pairs((bucket["defined-tags"]))
        

        for namespace, tagDict in bucket["defined-tags"].items():
            if namespace == "Oracle-Tags":
                continue
            for tag_key, tag_value in tagDict.items():
                resource_type = "OCIBucket"
                utils.attach_tag_to_resource(neo4j_session, current_oci_tenancy_id, bucket["id"], resource_type, namespace, tag_key, tag_value)

def sync_oci_policy_bucket_references(
    neo4j_session: neo4j.Session,
    tenancy_id: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any],
) -> None:
    policies = list(utils.get_policies_in_tenancy(neo4j_session, tenancy_id))
    for policy in policies:
        for statement in policy["statements"]:
            m = re.search(r"target\.bucket\.name='([^']+)'", statement)
            if m:
                for bucket in buckets:
                    if(bucket.name.lower() == m.group(1).lower()):
                        load_oci_policy_bucket_reference(neo4j_session, policy["ocid"], bucket.id, tenancy_id, oci_update_tag)

    for bucket in buckets:
        match = re.search(r'\.oc1\.([a-zA-Z0-9\-]+)\.', bucket.id)
        if match:
            bucket_region_name = match.group(1)
            bucket_region_key  = REGIONS[bucket_region_name]
            load_oci_bucket_region_reference(neo4j_session, bucket.id, bucket_region_key, tenancy_id, oci_update_tag)
        

def load_oci_policy_bucket_reference(
    neo4j_session: neo4j.Session,
    policy_id: str,
    bucket_id: str,
    tenancy_id: str,
    oci_update_tag: int,
) -> None:

    ingest_policy_group_reference = """
    MERGE (aa:OCIPolicy {ocid: $POLICY_ID})
    MERGE (bb:OCIBucket {ocid: $BUCKET_ID})
    MERGE (aa)-[r:OCI_BUCKET_POLICY_REFERENCE]->(bb)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $oci_update_tag
    """
    neo4j_session.run(
        ingest_policy_group_reference,
        POLICY_ID=policy_id,
        BUCKET_ID=bucket_id,
        oci_update_tag=oci_update_tag,
    )

def load_oci_bucket_region_reference(
    neo4j_session: neo4j.Session,
    bucket_id: str,
    region_key: str,
    tenancy_id: str,
    oci_update_tag: int,
) -> None:

    ingest_policy_group_reference = """
    MERGE (aa:OCIBucket {ocid: $BUCKET_ID})
    MERGE (bb:OCIRegion {key: $REGION_KEY})
    MERGE (aa)-[r:LOCATED_IN]->(bb)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $oci_update_tag
    """
    neo4j_session.run(
        ingest_policy_group_reference,
        BUCKET_ID=bucket_id,
        REGION_KEY=region_key,
        oci_update_tag=oci_update_tag,
    )



def sync(
    neo4j_session: neo4j.Session,
    objectStorage: oci.object_storage.ObjectStorageClient,
    tenancy_id: str,
    region: str,
    oci_update_tag: int,
    common_job_parameters: Dict[str, Any],
    regions_subscribed: List
) -> None:
    global REGIONS
    REGIONS = regions_subscribed
    logger.info("Syncing Object Storage for account '%s'.", tenancy_id)
    sync_buckets(neo4j_session, objectStorage, tenancy_id, region, oci_update_tag, common_job_parameters)
    sync_oci_policy_bucket_references(neo4j_session, tenancy_id, oci_update_tag, common_job_parameters)