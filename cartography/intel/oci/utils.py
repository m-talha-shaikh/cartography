# Copyright (c) 2020, Oracle and/or its affiliates.
# OCI intel module - utility functions
import json
from typing import Any
from typing import Dict
from typing import List

import neo4j


# Generic way to turn a OCI python object into the json response that you would see from calling the REST API.
def oci_object_to_json(in_obj: Any) -> List[Dict[str, Any]]:
    out_list = []
    for dict in json.loads(str(in_obj)):
        out_list.append(replace_char_in_dict(dict))
    return out_list


# Have to replace _ with - in dictionary keys, since _ is substituted for - in OCI object variables.
def replace_char_in_dict(in_dict: Dict[str, Any]) -> Dict[str, Any]:
    out_dict = {}
    for dict_key, dict_val in in_dict.items():
        if isinstance(dict_val, dict):
            dict_val = replace_char_in_dict(dict_val)
        out_dict[dict_key.replace('_', '-')] = dict_val
    return out_dict


# Grab list of all compartments and sub-compartments in neo4j already populated by iam.
def get_compartments_in_tenancy(neo4j_session: neo4j.Session, tenancy_id: str) -> neo4j.Result:
    query = "MATCH (OCITenancy{ocid: $OCI_TENANCY_ID})-[*]->(compartment:OCICompartment) " \
            "return DISTINCT compartment.name as name, compartment.ocid as ocid, " \
            "compartment.compartmentid as compartmentid;"
    return neo4j_session.run(query, OCI_TENANCY_ID=tenancy_id)


# Grab list of all groups in neo4j already populated by iam.
def get_groups_in_tenancy(neo4j_session: neo4j.Session, tenancy_id: str) -> neo4j.Result:
    query = "MATCH (OCITenancy{ocid: $OCI_TENANCY_ID})-[*]->(group:OCIGroup)" \
            "return DISTINCT group.name as name, group.ocid as ocid;"
    return neo4j_session.run(query, OCI_TENANCY_ID=tenancy_id)


# Grab list of all policies in neo4j already populated by iam.
def get_policies_in_tenancy(neo4j_session: neo4j.Session, tenancy_id: str) -> neo4j.Result:
    query = "MATCH (OCITenancy{ocid: $OCI_TENANCY_ID})-[*]->(policy:OCIPolicy)" \
            "return DISTINCT policy.name as name, policy.ocid as ocid, policy.statements as statements, " \
            "policy.compartmentid as compartmentid;"
    return neo4j_session.run(query, OCI_TENANCY_ID=tenancy_id)


# Grab list of all regions in neo4j already populated by iam.
def get_regions_in_tenancy(neo4j_session: neo4j.Session, tenancy_id: str) -> neo4j.Result:
    query = "MATCH (OCITenancy{ocid: $OCI_TENANCY_ID})-->(region:OCIRegion)" \
            "return DISTINCT region.name as name, region.key as key;"
    return neo4j_session.run(query, OCI_TENANCY_ID=tenancy_id)


def attach_tag_to_resource(neo4j_session: neo4j.Session, tenancy_id: str, resource_id: str, resource_type: str,  namespace: str, tag_key: str, tag_value) -> neo4j.Result:
    tag_id = tag_key + ":" + tag_value

    query = """
        MERGE(tnode:OCITag{ocid: $OCI_TAG_ID})
        ON CREATE SET tnode:OCITag, tnode.firstseen = timestamp(),
        tnode.key = $TAG_KEY, tnode.value = $TAG_VALUE, tnode.namespace = $NAMESPACE
        SET tnode.name = $OCI_TAG_ID
        WITH tnode
        MATCH (aa)
        WHERE aa.ocid = $OCI_RESOURCE_ID AND labels(aa) = [$RESOURCE_TYPE]
        MERGE (aa)-[r:TAGGED]->(tnode)
    """
    return neo4j_session.run(query, OCI_TAG_ID=tag_id, TAG_KEY=tag_key, TAG_VALUE=tag_value, NAMESPACE=namespace, OCI_RESOURCE_ID=resource_id, RESOURCE_TYPE=resource_type)

# def extract_key_value_pairs(d):
#     EXLCUDED_TAGS = {"CreatedOn", "CreatedBy"}
#     results = {}
#     for key, value in d.items():
#         if key in EXLCUDED_TAGS:
#             continue
#         if isinstance(value, dict):
#             # If the value is a dictionary, recursively extract key-value pairs from it
#             results.update(extract_key_value_pairs(value))
#         else:
#             results[key] = value
#     return results

# def extract_namespace_tags(d):
#     results = []
#     for namespace, tagDict in d.items():
#         if namespace == "Oracle-Tags":
#             continue
#         results.append({namespace: tagDict})
#     return results

def extract_namespace_tags2(d):
    results = []
    for namespace, tagDict in d.items():
        if namespace == "Oracle-Tags":
            continue
        for key, value in tagDict.items():
            results.append(str(namespace) + ":" + str(key) + ":" + str(value))
    return results
            
# def extract_namespace_tags(d):
#     EXLCUDED_TAGS = {"CreatedOn", "CreatedBy"}
#     results = []
#     for namespace, tagDict in d.items():
#         tempDict = {}
#         for key2, value2 in tagDict.items():
#             results.append({"namespace" : namespace , key2: value2})
#     return results



# Grab list of all security groups in neo4j already populated by network. Need to handle regions for this one.
def get_security_groups_in_tenancy(
    neo4j_session: neo4j.Session,
    tenancy_id: str, region: str,
) -> neo4j.Result:
    query = "MATCH (OCITenancy{ocid: $OCI_TENANCY_ID})-[*]->(security_group:OCINetworkSecurityGroup)-[OCI_REGION]->" \
            "(region:OCIRegion{name: $OCI_REGION})" \
            "return DISTINCT security_group.name as name, security_group.ocid as ocid, security_group.compartmentid " \
            "as compartmentid;"
    return neo4j_session.run(query, OCI_TENANCY_ID=tenancy_id, OCI_REGION=region)
