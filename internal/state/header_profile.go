package state

// HeaderReaderProfile is a pure catalog of declared main-header values. It
// opens no engine, validates no ledger and establishes no current schema.
type HeaderReaderProfile struct {
	ApplicationID   uint32
	MinReaderSchema uint32
	MaxReaderSchema uint32
}

func MainHeaderReaderProfile() HeaderReaderProfile {
	return HeaderReaderProfile{ApplicationID: applicationID, MinReaderSchema: 4, MaxReaderSchema: maxStateSchemaVersion}
}
