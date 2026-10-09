package state

// Unknown older rows and callers that omit provenance remain conservative.
// Production queue insertion names inventory_claimed=0 explicitly. Claiming
// records 1 in the same transaction as the lease, and no release clears it.
const migration14 = `
ALTER TABLE jobs ADD COLUMN inventory_claimed INTEGER NOT NULL DEFAULT 1
 CHECK(typeof(inventory_claimed)='integer' AND inventory_claimed IN (0,1));
`
