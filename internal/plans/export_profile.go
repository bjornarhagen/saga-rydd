package plans

import (
	"fmt"
	"sort"
)

// ExportSchemaObject is a bounded known-schema census. It contains no authority
// and never opens, initializes or migrates a store.
type ExportSchemaObject struct{ Name, Type, Table, SQL string }

// ExportProfile returns an independent copy for existing supported schemas.
// Stored record semantics remain outside this structural profile.
func ExportProfile(version int64) (int64, []ExportSchemaObject, bool) {
	if version < 1 || version > 6 {
		return 0, nil, false
	}
	objects := []ExportSchemaObject{{"plans", "table", "plans", ""}, {"plans_no_update", "trigger", "plans", ""}, {"plans_no_delete", "trigger", "plans", ""}}
	add := func(table string, triggers ...string) {
		objects = append(objects, ExportSchemaObject{table, "table", table, ""})
		for _, n := range triggers {
			objects = append(objects, ExportSchemaObject{n, "trigger", table, ""})
		}
	}
	if version >= 2 {
		add("plan_store_identity", "plan_store_identity_no_update", "plan_store_identity_no_delete")
		add("review_approvals", "review_approvals_no_update", "review_approvals_no_delete")
		add("review_revocations", "review_revocations_no_update", "review_revocations_no_delete")
	}
	if version >= 3 {
		add("observations", "observations_no_update", "observations_no_delete")
	}
	if version >= 4 {
		add("journal_intents", "journal_intents_no_update", "journal_intents_no_delete")
		objects = append(objects, ExportSchemaObject{"journal_restore_once", "index", "journal_intents", ""})
		add("journal_events", "journal_events_no_update", "journal_events_no_delete")
		add("journal_heads", "journal_heads_no_update", "journal_heads_no_delete")
	}
	if version >= 5 {
		add("dismissal_store_identity", "dismissal_store_identity_no_update", "dismissal_store_identity_no_delete")
		add("finding_dismissals", "finding_dismissals_no_update", "finding_dismissals_no_delete", "finding_dismissals_capacity")
		add("finding_dismissal_undos", "finding_dismissal_undos_no_update", "finding_dismissal_undos_no_delete")
	}
	if version >= 6 {
		objects = append(objects, ExportSchemaObject{"plans_capacity", "trigger", "plans", admissionTrigger})
	}
	sql := map[string]string{
		"dismissal_store_identity":           "CREATE TABLE dismissal_store_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), token TEXT NOT NULL CHECK(length(token)=64))",
		"dismissal_store_identity_no_delete": "CREATE TRIGGER dismissal_store_identity_no_delete BEFORE DELETE ON dismissal_store_identity BEGIN SELECT RAISE(ABORT,'dismissal identity is immutable'); END",
		"dismissal_store_identity_no_update": "CREATE TRIGGER dismissal_store_identity_no_update BEFORE UPDATE ON dismissal_store_identity BEGIN SELECT RAISE(ABORT,'dismissal identity is immutable'); END",
		"finding_dismissal_undos":            "CREATE TABLE finding_dismissal_undos (dismissal_id TEXT PRIMARY KEY REFERENCES finding_dismissals(id), id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=4096))",
		"finding_dismissal_undos_no_delete":  "CREATE TRIGGER finding_dismissal_undos_no_delete BEFORE DELETE ON finding_dismissal_undos BEGIN SELECT RAISE(ABORT,'dismissal undo records are immutable'); END",
		"finding_dismissal_undos_no_update":  "CREATE TRIGGER finding_dismissal_undos_no_update BEFORE UPDATE ON finding_dismissal_undos BEGIN SELECT RAISE(ABORT,'dismissal undo records are immutable'); END",
		"finding_dismissals":                 "CREATE TABLE finding_dismissals (id TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=262144))",
		"finding_dismissals_capacity":        "CREATE TRIGGER finding_dismissals_capacity BEFORE INSERT ON finding_dismissals WHEN (SELECT count(*) FROM finding_dismissals)>=128 BEGIN SELECT RAISE(ABORT,'dismissal capacity reached'); END",
		"finding_dismissals_no_delete":       "CREATE TRIGGER finding_dismissals_no_delete BEFORE DELETE ON finding_dismissals BEGIN SELECT RAISE(ABORT,'dismissals are immutable'); END",
		"finding_dismissals_no_update":       "CREATE TRIGGER finding_dismissals_no_update BEFORE UPDATE ON finding_dismissals BEGIN SELECT RAISE(ABORT,'dismissals are immutable'); END",
		"journal_events":                     "CREATE TABLE journal_events (intent_id TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence BETWEEN 1 AND 16), id TEXT UNIQUE NOT NULL, request_key TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=32768), PRIMARY KEY(intent_id,sequence))",
		"journal_events_no_delete":           "CREATE TRIGGER journal_events_no_delete BEFORE DELETE ON journal_events BEGIN SELECT RAISE(ABORT,'journal events are immutable'); END",
		"journal_events_no_update":           "CREATE TRIGGER journal_events_no_update BEFORE UPDATE ON journal_events BEGIN SELECT RAISE(ABORT,'journal events are immutable'); END",
		"journal_heads":                      "CREATE TABLE journal_heads (intent_id TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence BETWEEN 0 AND 16), event_id TEXT NOT NULL, PRIMARY KEY(intent_id,sequence))",
		"journal_heads_no_delete":            "CREATE TRIGGER journal_heads_no_delete BEFORE DELETE ON journal_heads BEGIN SELECT RAISE(ABORT,'journal heads are immutable'); END",
		"journal_heads_no_update":            "CREATE TRIGGER journal_heads_no_update BEFORE UPDATE ON journal_heads BEGIN SELECT RAISE(ABORT,'journal heads are immutable'); END",
		"journal_intents":                    "CREATE TABLE journal_intents (id TEXT PRIMARY KEY, request_key TEXT UNIQUE NOT NULL, kind TEXT NOT NULL, plan_id TEXT NOT NULL, observation_id TEXT NOT NULL, finding_id TEXT NOT NULL, original_intent_id TEXT NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=32768), UNIQUE(observation_id,finding_id,kind))",
		"journal_intents_no_delete":          "CREATE TRIGGER journal_intents_no_delete BEFORE DELETE ON journal_intents BEGIN SELECT RAISE(ABORT,'journal intents are immutable'); END",
		"journal_intents_no_update":          "CREATE TRIGGER journal_intents_no_update BEFORE UPDATE ON journal_intents BEGIN SELECT RAISE(ABORT,'journal intents are immutable'); END",
		"journal_restore_once":               "CREATE UNIQUE INDEX journal_restore_once ON journal_intents(original_intent_id) WHERE kind='restore_preparation'",
		"observations":                       "CREATE TABLE observations (id TEXT PRIMARY KEY, plan_id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=65536))",
		"observations_no_delete":             "CREATE TRIGGER observations_no_delete BEFORE DELETE ON observations BEGIN SELECT RAISE(ABORT,'observations are immutable'); END",
		"observations_no_update":             "CREATE TRIGGER observations_no_update BEFORE UPDATE ON observations BEGIN SELECT RAISE(ABORT,'observations are immutable'); END",
		"plan_store_identity":                "CREATE TABLE plan_store_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), token TEXT NOT NULL CHECK(length(token)=64))",
		"plan_store_identity_no_delete":      "CREATE TRIGGER plan_store_identity_no_delete BEFORE DELETE ON plan_store_identity BEGIN SELECT RAISE(ABORT,'store identity is immutable'); END",
		"plan_store_identity_no_update":      "CREATE TRIGGER plan_store_identity_no_update BEFORE UPDATE ON plan_store_identity BEGIN SELECT RAISE(ABORT,'store identity is immutable'); END",
		"plans":                              "CREATE TABLE plans (id TEXT PRIMARY KEY, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=1048576))",
		"plans_no_delete":                    "CREATE TRIGGER plans_no_delete BEFORE DELETE ON plans BEGIN SELECT RAISE(ABORT,'plans are immutable'); END",
		"plans_no_update":                    "CREATE TRIGGER plans_no_update BEFORE UPDATE ON plans BEGIN SELECT RAISE(ABORT,'plans are immutable'); END",
		"review_approvals":                   "CREATE TABLE review_approvals (plan_id TEXT PRIMARY KEY, id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=16384))",
		"review_approvals_no_delete":         "CREATE TRIGGER review_approvals_no_delete BEFORE DELETE ON review_approvals BEGIN SELECT RAISE(ABORT,'approvals are immutable'); END",
		"review_approvals_no_update":         "CREATE TRIGGER review_approvals_no_update BEFORE UPDATE ON review_approvals BEGIN SELECT RAISE(ABORT,'approvals are immutable'); END",
		"review_revocations":                 "CREATE TABLE review_revocations (plan_id TEXT PRIMARY KEY, id TEXT UNIQUE NOT NULL, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=16384))",
		"review_revocations_no_delete":       "CREATE TRIGGER review_revocations_no_delete BEFORE DELETE ON review_revocations BEGIN SELECT RAISE(ABORT,'revocations are immutable'); END",
		"review_revocations_no_update":       "CREATE TRIGGER review_revocations_no_update BEFORE UPDATE ON review_revocations BEGIN SELECT RAISE(ABORT,'revocations are immutable'); END",
	}
	for i := range objects {
		if objects[i].Name != "plans_capacity" {
			objects[i].SQL = sql[objects[i].Name]
		}
	}
	auto := map[string]int{"plans": 1}
	if version >= 2 {
		auto["review_approvals"] = 2
		auto["review_revocations"] = 2
	}
	if version >= 3 {
		auto["observations"] = 2
	}
	if version >= 4 {
		auto["journal_intents"] = 3
		auto["journal_events"] = 3
		auto["journal_heads"] = 1
	}
	if version >= 5 {
		auto["finding_dismissals"] = 2
		auto["finding_dismissal_undos"] = 2
	}
	for table, count := range auto {
		for i := 1; i <= count; i++ {
			objects = append(objects, ExportSchemaObject{fmt.Sprintf("sqlite_autoindex_%s_%d", table, i), "index", table, ""})
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Name < objects[j].Name })
	return applicationID, objects, true
}
