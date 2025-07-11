package types

type TssSigRequest struct {
	SessionID  string `json:"sessionId"`
	UnsignHash []byte `json:"unsignHash"` // the hash to be signed, usually a transaction hash, or a message hash (should start with \x19Ethereum Signed Message:\n%d)
}

type TssSigResponse struct {
	SessionID string `json:"sessionId"`
	Success   bool   `json:"success"` // indicates if the signing was successful
	Message   string `json:"message"` // additional message, if any
	RawSig    []byte `json:"rawSig"`  // raw signature for the aim chain, such as evm: [65]byte 0-31 r, 32-63 s, 64 v
}
