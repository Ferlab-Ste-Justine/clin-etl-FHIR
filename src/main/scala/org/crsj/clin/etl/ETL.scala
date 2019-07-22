package org.crsj.clin.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, expr}
import org.crsj.clin.etl.DataFrameUtils.{joinAggregateFirst, joinAggregateList}
import org.elasticsearch.spark.sql._


object ETL {

  def run(base: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val organizations = Organization.load(base)
    val familyMemberHistory = FamilyMemberHistory.load(base)
    val patients = Patient.load(base)
    val specimens = Specimen.load(base)
    val practitionerWithRolesAndOrg = Practitioners.load(base, organizations)
    val observationsWithPerformer = Observation.load(base, practitionerWithRolesAndOrg)
    val clinicalImpressionsWithAssessor_practitionerWithRoleAndOrg = ClinicalImpression.load(base, practitionerWithRolesAndOrg)
    val fullClinicalImpressionsWithObservations =
      joinAggregateList(clinicalImpressionsWithAssessor_practitionerWithRoleAndOrg, observationsWithPerformer,
        expr("array_contains(iiu.uri, obs_id)"), "observations")
    val fullClinicalImpressionsWithObservationsAndFMH =
      joinAggregateList(fullClinicalImpressionsWithObservations, familyMemberHistory,
        expr("array_contains(iiu.uri, fmh_id)"), "familyMemberHistory")
    val serviceRequest = ServiceRequest.load(base, practitionerWithRolesAndOrg, fullClinicalImpressionsWithObservationsAndFMH)
    val studyWithPatients = Study.load(base)
    val groups = Group.load(base)

    //val withObservations = joinAggregateList(patients, observationsWithPerformer, patients("id") === $"subject.id", "observations")
    val withPractitioners = joinAggregateList(patients, practitionerWithRolesAndOrg, expr("array_contains(generalPractitioner.id, role_id)"), "practitioners")
    val withSpecimens = joinAggregateList(withPractitioners, specimens, withPractitioners("id") === $"subject.id", "specimens")
    val withClinicalImpressions = joinAggregateList(withSpecimens, fullClinicalImpressionsWithObservationsAndFMH, withSpecimens("id") === $"subject.id", "clinicalImpressions")
    val withOrganizations = joinAggregateFirst(withClinicalImpressions, organizations, withClinicalImpressions("managingOrganization.id") === organizations("id"), "organization")
    val withServiceRequest = joinAggregateList(withOrganizations, serviceRequest, withOrganizations("id") === $"subject.id", "serviceRequests")
    val withStudy = joinAggregateList(withServiceRequest, studyWithPatients, withServiceRequest("id") === $"patient.entity.id", "studies")
    val withGroup = joinAggregateList(withStudy, groups, withStudy("id") === $"patient.entity.id", "group")
    //val withFamilyMemberHistory = joinAggregateList(withStudy, familyMemberHistory, withStudy("id") === $"patient.id", "familyMemberHistory")
    val group = DataFrameUtils.load(s"$base/group.ndjson", $"id" as "group_id", explode($"member") as "patient")
    val study = DataFrameUtils.load(s"$base/study.ndjson", $"id" as "study_id", $"title", explode($"enrollment") as "enrollment")

    withGroup.saveToEs("temp/temp", Map("es.mapping.id" -> "id"))
    groups.saveToEs("studies/groups", Map("es.mapping.id" -> "study_id"))
    group.saveToEs("group/group", Map("es.mapping.id" -> "group_id"))
    study.saveToEs("study/study", Map("es.mapping.id" -> "study_id"))

    //withFamilyMemberHistory.saveToEs("patient/patient", Map("es.mapping.id" -> "id"))

  }

}
