// approved-domains-only.cedar — Allowlisted domain access control
//
// Intended for: principals whose data access is restricted to a specific
// set of pre-approved data domains (e.g. "genomics", "climate", "public").
//
// This template enforces that every operation — discover, probe, plan,
// and excavate — only proceeds when the resource domain is in the
// principal's approved_domains list. Export is also domain-gated.
//
// Deploy alongside the base default.cedar or as a replacement for
// tenants that require strict domain containment.

// Discover — only within approved domains
permit(
  principal,
  action == Action::"discover",
  resource
)
when {
  context.scope.domains.containsAll(principal.approved_domains) &&
  principal.approved_domains.contains(context.scope.primary_domain)
};

// Probe — resource domain must be in principal's approved list
permit(
  principal,
  action == Action::"probe",
  resource
)
when {
  resource in principal.approved_sources &&
  principal.approved_domains.contains(resource.domain) &&
  context.sample_rows <= principal.max_sample_rows
};

// Plan — source domain allowlisted
permit(
  principal,
  action == Action::"plan",
  resource
)
when {
  resource in principal.approved_sources &&
  principal.approved_domains.contains(resource.domain) &&
  context.constraints.read_only == true &&
  context.constraints.max_cost_dollars <= principal.max_cost_per_excavation
};

// Excavate — plan linked and domain allowlisted
permit(
  principal,
  action == Action::"excavate",
  resource
)
when {
  context.read_only == true &&
  context.max_bytes_scanned <= principal.byte_scan_limit &&
  resource in principal.approved_sources &&
  principal.approved_domains.contains(resource.domain) &&
  context.plan_id != ""
};

// Refine — own results only
permit(
  principal,
  action == Action::"refine",
  resource
)
when {
  resource.owner == principal &&
  context.top_k <= principal.max_refine_top_k
};

// Export — destination and domain both allowlisted
permit(
  principal,
  action == Action::"export",
  resource
)
when {
  context.destination.uri in principal.approved_export_targets &&
  resource.owner == principal &&
  principal.approved_domains.contains(resource.domain)
};

// Deny any operation on resources outside the approved domain set
forbid(
  principal,
  action,
  resource
)
when {
  resource has domain &&
  !principal.approved_domains.contains(resource.domain)
};
