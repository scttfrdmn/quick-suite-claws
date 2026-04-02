// read-only.cedar — Read-only access template
//
// Intended for: analysts, data consumers, and any principal whose role
// is limited to exploring and planning queries but must never run them.
//
// Permits: discover, probe, plan
// Denies:  excavate, export (all forms)
//
// Apply this template by attaching it to a principal group in AgentCore
// Policy. Cedar is additive — this template does not grant permissions
// from default.cedar; it must be combined with it or replace it.

// Allow data discovery within the principal's approved spaces and domains
permit(
  principal,
  action == Action::"discover",
  resource
)
when {
  context.scope.spaces.containsAll(principal.approved_spaces) &&
  context.scope.domains.containsAll(principal.approved_domains)
};

// Allow schema inspection and sampling; honour row limits
permit(
  principal,
  action == Action::"probe",
  resource
)
when {
  resource in principal.approved_sources &&
  context.sample_rows <= principal.max_sample_rows
};

// Allow query planning (no execution); enforce cost and read-only constraints
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

// Explicitly forbid excavation — read-only principals must not execute queries
forbid(
  principal,
  action == Action::"excavate",
  resource
);

// Explicitly forbid all export actions — no data materialisation allowed
forbid(
  principal,
  action == Action::"export",
  resource
);
