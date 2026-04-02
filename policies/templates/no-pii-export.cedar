// no-pii-export.cedar — Full access with PII export prevention
//
// Intended for: general research analysts who can run queries but must
// never export results that carry PII tags (e.g. NAME, SSN, EMAIL).
//
// Permits: all six pipeline tools
// Denies:  export when result contains any PII classification tags
//
// The pii_tags attribute is set by the refine tool when Bedrock Guardrails
// detects PII entities in the result set. Cedar checks this structural
// attribute at the export boundary — Guardrails provides the semantic scan.

// Discover — within approved scope
permit(
  principal,
  action == Action::"discover",
  resource
)
when {
  context.scope.spaces.containsAll(principal.approved_spaces) &&
  context.scope.domains.containsAll(principal.approved_domains)
};

// Probe — approved sources, row limit
permit(
  principal,
  action == Action::"probe",
  resource
)
when {
  resource in principal.approved_sources &&
  context.sample_rows <= principal.max_sample_rows
};

// Plan — read-only, cost bounded
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

// Excavate — linked plan, byte-scan bounded
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

// Refine — own results, top_k bounded
permit(
  principal,
  action == Action::"refine",
  resource
)
when {
  resource.owner == principal &&
  context.top_k <= principal.max_refine_top_k
};

// Export — allowed only when the result carries no PII tags
permit(
  principal,
  action == Action::"export",
  resource
)
when {
  context.destination.uri in principal.approved_export_targets &&
  resource.owner == principal &&
  resource.pii_tags.isEmpty()
};

// Deny export when the result set has PII classification tags
forbid(
  principal,
  action == Action::"export",
  resource
)
when {
  !resource.pii_tags.isEmpty()
};
