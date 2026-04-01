// Cedar policy for restricted datasets (PHI, PII, HIPAA)
// Requires HITL approval for all excavations and exports

// Discovery is allowed — seeing that a dataset exists is not sensitive
permit(
  principal,
  action == Action::"discover",
  resource in ResourceGroup::"restricted"
)
when {
  principal.clearance_level >= 2
};

// Probe allowed but with minimal samples
permit(
  principal,
  action == Action::"probe",
  resource in ResourceGroup::"restricted"
)
when {
  principal.clearance_level >= 2 &&
  context.sample_rows <= 3
};

// Plan allowed — no execution, just query generation
permit(
  principal,
  action == Action::"plan",
  resource in ResourceGroup::"restricted"
)
when {
  principal.clearance_level >= 2 &&
  context.constraints.read_only == true
};

// Excavation REQUIRES HITL approval
permit(
  principal,
  action == Action::"excavate",
  resource in ResourceGroup::"restricted"
)
when {
  context.read_only == true &&
  context.plan_id != "" &&
  context.hitl_approval_id != "" &&
  context.max_bytes_scanned <= 1000000000 &&
  principal.clearance_level >= 2
};

// Export REQUIRES HITL approval AND restricted destinations only
permit(
  principal,
  action == Action::"export",
  resource in ResourceGroup::"restricted"
)
when {
  context.hitl_approval_id != "" &&
  context.destination.uri in principal.restricted_export_targets &&
  context.include_provenance == true
};

// Blanket deny for restricted resources without clearance
forbid(
  principal,
  action,
  resource in ResourceGroup::"restricted"
)
when {
  principal.clearance_level < 2
};
