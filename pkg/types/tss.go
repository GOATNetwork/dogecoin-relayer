package types

type TssSigRequest struct {
	SessionID  string `json:"sessionId"`
	UnsignHash []byte `json:"unsignHash"`
}

type TssSigResponse struct {
	SessionID string `json:"sessionId"`
	RawSig    []byte `json:"rawSig"` // raw signature for the aim chain, such as evm: [65]byte 0-31 r, 32-63 s, 64 v
}
