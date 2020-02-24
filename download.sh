#!/bin/bash
set -xe

mkdir -p ndjson

fhir_server="http://localhost:8888"
fhir_credential="fhir_import:01b99f28-1331-4fec-903b-c2e8043cec77"

curl -u $fhir_credential $fhir_server/Group/\$dump > ndjson/group.ndjson
curl -u $fhir_credential $fhir_server/Patient/\$dump > ndjson/pt.ndjson
curl -u $fhir_credential $fhir_server/Practitioner/\$dump > ndjson/pr.ndjson
curl -u $fhir_credential $fhir_server/PractitionerRole/\$dump > ndjson/prr.ndjson
curl -u $fhir_credential $fhir_server/Organization/\$dump > ndjson/org.ndjson
curl -u $fhir_credential $fhir_server/Observation/\$dump > ndjson/obs.ndjson
curl -u $fhir_credential $fhir_server/ServiceRequest/\$dump > ndjson/sr.ndjson
curl -u $fhir_credential $fhir_server/Specimen/\$dump > ndjson/sp.ndjson
curl -u $fhir_credential $fhir_server/ClinicalImpression/\$dump > ndjson/ci.ndjson
curl -u $fhir_credential $fhir_server/ResearchStudy/\$dump > ndjson/study.ndjson
curl -u $fhir_credential $fhir_server/FamilyMemberHistory/\$dump > ndjson/fmh.ndjson
