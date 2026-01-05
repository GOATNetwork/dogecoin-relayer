package types

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractAddressFromScript_NullData(t *testing.T) {
	// OP_RETURN output from testnet block bba64b67a7cf6d4ddbd413ba4674b25f995351283817aec46872b39e976cf53f
	scriptHex := "6a18475145564fbeca5a561bfbe82bd47e65cd9fb964b99b8d0d"
	script, err := hex.DecodeString(scriptHex)
	assert.NoError(t, err)

	addr, err := ExtractAddressFromScript(script, GetDogeNetwork("testnet"))
	assert.NoError(t, err)
	assert.Empty(t, addr)
}
