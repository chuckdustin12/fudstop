from apis.newyork_fed.newyork_fed_sdk import FedNewyork


fed = FedNewyork()


agency_mbs = fed.agency_mbs_count()

print(agency_mbs.operationDate, agency_mbs.totalAcceptedOrigFace)