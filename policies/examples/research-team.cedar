// Cedar policy for a research team with broad discovery
// but bounded excavation costs

permit(
  principal in Group::"research-team",
  action == Action::"discover",
  resource
)
when {
  context.scope.spaces.containsAny(
    ["research-public", "genomics-shared", "proteomics-shared"]
  )
};

permit(
  principal in Group::"research-team",
  action == Action::"probe",
  resource
)
when {
  resource in [
    Source::"athena:genomics.*",
    Source::"athena:proteomics.*",
    Source::"s3:research-public/*"
  ] &&
  context.sample_rows <= 20
};

permit(
  principal in Group::"research-team",
  action == Action::"plan",
  resource
)
when {
  context.constraints.read_only == true &&
  context.constraints.max_cost_dollars <= 5.00
};

permit(
  principal in Group::"research-team",
  action == Action::"excavate",
  resource
)
when {
  context.read_only == true &&
  context.max_bytes_scanned <= 5000000000 &&
  context.plan_id != "" &&
  resource in principal.approved_sources
};

permit(
  principal in Group::"research-team",
  action == Action::"export",
  resource
)
when {
  context.destination.uri.startsWith("s3://research-outputs/") ||
  context.destination.uri.startsWith("s3://team-shared/")
};
