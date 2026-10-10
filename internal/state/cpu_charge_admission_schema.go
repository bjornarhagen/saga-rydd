package state

// Only explicit positive-profile activation publishes schema 16. Ordinary
// inventory initialization remains schema 14 and bare CPU activation remains 15.
const cpuChargeAdmissionSchemaVersion = 16
const CPUChargeAdmissionMaxJSONBytes = 4096

const migration16 = `
CREATE TABLE worker_cpu_charge_admission (
 singleton INTEGER PRIMARY KEY CHECK(typeof(singleton)='integer' AND singleton=1),
 policy_revision INTEGER NOT NULL CHECK(typeof(policy_revision)='integer' AND policy_revision>=0),
 state_json BLOB NOT NULL CHECK(typeof(state_json)='blob' AND length(state_json) BETWEEN 1 AND 4096)
);
`
