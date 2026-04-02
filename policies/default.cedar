// clAWS default Cedar policies
// Deployed to AgentCore Policy for all principals

// ============================================================
// DISCOVER — allow within approved spaces
// ============================================================
permit(
  principal,
  action == Action::"discover",
  resource
)
when {
  context.scope.spaces.containsAll(principal.approved_spaces) &&
  context.scope.domains.containsAll(principal.approved_domains)
};

// ============================================================
// PROBE — allow on approved sources, cap sample rows
// ============================================================
permit(
  principal,
  action == Action::"probe",
  resource
)
when {
  resource in principal.approved_sources &&
  context.sample_rows <= principal.max_sample_rows
};

// ============================================================
// PLAN — allow with validated constraints
// ============================================================
permit(
  principal,
  action == Action::"plan",
  resource
)
when {
  resource in principal.approved_sources &&
  context.constraints.read_only == true &&
  context.constraints.max_cost_dollars <= principal.max_cost_per_excavation
};

// ============================================================
// EXCAVATE — the strictest gate
// ============================================================
permit(
  principal,
  action == Action::"excavate",
  resource
)
when {
  context.read_only == true &&
  context.max_bytes_scanned <= principal.byte_scan_limit &&
  resource in principal.approved_sources &&
  context.plan_id != ""
};

// ============================================================
// REFINE — allow on own results, bound top_k
// ============================================================
permit(
  principal,
  action == Action::"refine",
  resource
)
when {
  resource.owner == principal &&
  context.top_k <= principal.max_refine_top_k
};

// ============================================================
// EXPORT — destination allowlist
// ============================================================
permit(
  principal,
  action == Action::"export",
  resource
)
when {
  context.destination.uri in principal.approved_export_targets &&
  resource.owner == principal
};

// Deny export to anything not in the allowlist
forbid(
  principal,
  action == Action::"export",
  resource
)
unless {
  context.destination.uri in principal.approved_export_targets
};

// Require HITL approval for cross-domain exports
permit(
  principal,
  action == Action::"export",
  resource
)
when {
  context.destination.cross_domain == true &&
  context.hitl_approval_id != ""
};

// ============================================================
// TEAM_PLANS — list plans by team_id (read-only)
// ============================================================
permit(
  principal,
  action == Action::"team_plans",
  resource
)
when {
  context.team_id in principal.team_ids
};

// ============================================================
// PLAN.SHARE — share a plan; only the plan owner may do this
// ============================================================
permit(
  principal,
  action == Action::"plan.share",
  resource
)
when {
  resource.created_by == principal.id
};
