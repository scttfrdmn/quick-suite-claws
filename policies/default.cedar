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
// PLAN.COLUMN_ACCESS — gate plan creation on column visibility
//
// When a source has columns tagged visibility=restricted or visibility=phi,
// the plan tool evaluates this action before building the LLM prompt.
// Principals without the appropriate role see only public columns.
//
// The resource is the source being planned against.
// context.column_visibility is the highest restriction level present:
//   "public"     — permit for all principals
//   "restricted" — permit only with pii_access role
//   "phi"        — permit only with phi_cleared role (includes pii_access)
//
// Note: when a principal lacks access, the plan tool filters restricted/phi
// columns from the schema before the LLM sees it. This action is evaluated
// at Gateway to enforce the structural access decision.
// ============================================================
permit(
  principal,
  action == Action::"plan.column_access",
  resource
)
when {
  context.column_visibility == "public"
};

permit(
  principal,
  action == Action::"plan.column_access",
  resource
)
when {
  context.column_visibility == "restricted" &&
  principal.roles.contains("pii_access")
};

permit(
  principal,
  action == Action::"plan.column_access",
  resource
)
when {
  context.column_visibility == "phi" &&
  principal.roles.contains("phi_cleared")
};

// Deny PHI column access without phi_cleared role
forbid(
  principal,
  action == Action::"plan.column_access",
  resource
)
when {
  context.column_visibility == "phi" &&
  !principal.roles.contains("phi_cleared")
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

// ============================================================
// PLAN.APPROVE — approve a pending IRB plan for execution
//
// The resource is the plan item. requires_irb is a boolean
// attribute on the source's policy context. Only principals
// with the irb_approver role may call this action.
//
// Structural rule: an approver cannot approve their own plan
// (created_by != principal.id). Semantic/content policy is
// enforced by the approve_plan Lambda handler.
// ============================================================
permit(
  principal,
  action == Action::"plan.approve",
  resource
)
when {
  principal.roles.contains("irb_approver") &&
  resource.requires_irb == true &&
  resource.status == "pending_approval" &&
  resource.created_by != principal.id
};

// Deny approval of one's own plan (conflict of interest)
forbid(
  principal,
  action == Action::"plan.approve",
  resource
)
when {
  resource.created_by == principal.id
};
