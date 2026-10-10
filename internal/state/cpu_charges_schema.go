package state

// Ordinary state writers stay at schema 14. Only ActivateCPUCharges publishes
// this optional extension; older schema-14 binaries then refuse the store.
const cpuChargesSchemaVersion = 15
const maxStateSchemaVersion = cpuChargeAdmissionSchemaVersion
const CPUChargesMaxJSONBytes = 8192

const migration15 = `
CREATE TABLE worker_self_cpu_charges (
 singleton INTEGER PRIMARY KEY CHECK(typeof(singleton)='integer' AND singleton=1),
 generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>=0),
 state_json BLOB NOT NULL CHECK(typeof(state_json)='blob' AND length(state_json) BETWEEN 1 AND 8192)
);
`
