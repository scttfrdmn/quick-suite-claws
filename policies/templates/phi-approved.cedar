// phi-approved.cedar — PHI-cleared principal access template
//
// Intended for: researchers and clinicians with IRB approval to access
// Protected Health Information (PHI) sources under HIPAA data use agreements.
//
// Permits: full pipeline on phi-classified sources; plan.approve for PHI plans
// Requires: plan.approve action before executing any phi plan (requires_irb)
// Denies:  access to phi sources for principals without phi_cleared role
//          approval of one's own plans (conflict of interest)
//
// This template works in conjunction with the approve_plan internal Lambda
// which enforces the CLAWS_IRB_APPROVERS allowlist at runtime.

// Discover — PHI-cleared principals may search within approved domains
permit(
  principal,
  action == Action::"discover",
  resource
)
when {
  principal.roles.contains("phi_cleared") &&
  context.scope.spaces.containsAll(principal.approved_spaces) &&
  context.scope.domains.containsAll(principal.approved_domains)
};

// Probe — allow PHI sources only for phi-cleared principals
permit(
  principal,
  action == Action::"probe",
  resource
)
when {
  principal.roles.contains("phi_cleared") &&
  resource in principal.approved_sources &&
  context.sample_rows <= principal.max_sample_rows
};

// Plan — PHI plans are stored with status pending_approval (requires_irb=true)
// This policy permits plan creation; the approve_plan Lambda enforces the gate.
permit(
  principal,
  action == Action::"plan",
  resource
)
when {
  principal.roles.contains("phi_cleared") &&
  resource in principal.approved_sources &&
  context.constraints.read_only == true &&
  context.constraints.max_cost_dollars <= principal.max_cost_per_excavation
};

// Excavate — allowed only after IRB approval (enforced by the handler checking
// plan status; this Cedar policy ensures the principal is phi_cleared at all)
permit(
  principal,
  action == Action::"excavate",
  resource
)
when {
  principal.roles.contains("phi_cleared") &&
  context.read_only == true &&
  context.max_bytes_scanned <= principal.byte_scan_limit &&
  resource in principal.approved_sources &&
  context.plan_id != ""
};

// Refine — PHI-cleared principals may refine their own results
permit(
  principal,
  action == Action::"refine",
  resource
)
when {
  principal.roles.contains("phi_cleared") &&
  resource.owner == principal &&
  context.top_k <= principal.max_refine_top_k
};

// Export — PHI results require cross-domain HITL approval
permit(
  principal,
  action == Action::"export",
  resource
)
when {
  principal.roles.contains("phi_cleared") &&
  context.destination.uri in principal.approved_export_targets &&
  resource.owner == principal &&
  context.hitl_approval_id != ""
};

// Plan approval — irb_approvers may approve phi plans belonging to others
permit(
  principal,
  action == Action::"plan.approve",
  resource
)
when {
  principal.roles.contains("irb_approver") &&
  resource.created_by != principal.id
};

// Hard deny: no approval of one's own plan
forbid(
  principal,
  action == Action::"plan.approve",
  resource
)
when {
  resource.created_by == principal.id
};

// Hard deny: non-phi_cleared principals cannot access phi-classified sources
forbid(
  principal,
  action,
  resource
)
when {
  resource has data_classification &&
  resource.data_classification == "phi" &&
  !principal.roles.contains("phi_cleared")
};
