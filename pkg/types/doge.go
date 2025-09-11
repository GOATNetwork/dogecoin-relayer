package types

import (
    "bytes"
    "encoding/base64"
    "encoding/hex"
    "fmt"

    "github.com/dogecoinw/doged/btcutil"
    "github.com/dogecoinw/doged/chaincfg"
    "github.com/dogecoinw/doged/chaincfg/chainhash"
    "github.com/dogecoinw/doged/txscript"
    "github.com/dogecoinw/doged/wire"
)

// DogeBlockExt represents an extended Dogecoin block with additional information
type DogeBlockExt struct {
	BlockNumber  int64
	BlockHash    chainhash.Hash
	Transactions []*wire.MsgTx
}

// GetBlockHash returns the block hash
func (b *DogeBlockExt) GetBlockHash() chainhash.Hash {
	return b.BlockHash
}

// GetDogeNetwork returns the Dogecoin network parameters based on network type
func GetDogeNetwork(networkType string) *chaincfg.Params {
	switch networkType {
	case "mainnet":
		return &chaincfg.MainNetParams
	case "testnet":
		return &chaincfg.TestNet3Params
	case "regtest":
		return &chaincfg.RegressionNetParams
	default:
		return &chaincfg.MainNetParams
	}
}

// GenerateP2PKHAddress generates a P2PKH address from public key bytes
func GenerateP2PKHAddress(pubkeyBytes []byte, network *chaincfg.Params) (string, error) {
	// Create a P2PKH address from the public key hash
	pubKeyHash := btcutil.Hash160(pubkeyBytes)
	addr, err := btcutil.NewAddressPubKeyHash(pubKeyHash, network)
	if err != nil {
		return "", fmt.Errorf("failed to create P2PKH address: %w", err)
	}

	return addr.String(), nil
}

// GenerateP2WPKHAddress generates a P2WPKH address from public key bytes
func GenerateP2WPKHAddress(pubkeyBytes []byte, network *chaincfg.Params) (string, error) {
	// Create a P2WPKH address from the public key hash
	pubKeyHash := btcutil.Hash160(pubkeyBytes)
	addr, err := btcutil.NewAddressWitnessPubKeyHash(pubKeyHash, network)
	if err != nil {
		return "", fmt.Errorf("failed to create P2WPKH address: %w", err)
	}

	return addr.String(), nil
}

// SerializeTransactionNoWitness serializes a transaction without witness data
func SerializeTransactionNoWitness(tx *wire.MsgTx) ([]byte, error) {
	var buf bytes.Buffer
	err := tx.SerializeNoWitness(&buf)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize transaction without witness: %w", err)
	}
	return buf.Bytes(), nil
}

// GenerateSPVProof generates SPV proof for a transaction
func GenerateSPVProof(txid string, blockTxHashes []string) (string, []byte, int, error) {
	// This is a simplified implementation
	// In a real implementation, you would need to build the merkle tree
	// and generate the actual proof

	// For now, return placeholder values
	merkleRoot := "placeholder_merkle_root"
	proofBytes := []byte("placeholder_proof")
	txIndex := 0

	return merkleRoot, proofBytes, txIndex, nil
}

// IsUtxoDogeDepositV1 checks if a transaction is a Dogecoin deposit
func IsUtxoDogeDepositV1(tx *wire.MsgTx, addresses []string, network *chaincfg.Params, minDepositAmount int64, magicBytes []byte) (bool, string, error) {
    // Require magic bytes to be configured to avoid false positives
    if len(magicBytes) == 0 {
        return false, "", nil
    }

    // Build a set of watch addresses for quick lookup
    addrSet := make(map[string]struct{}, len(addresses))
    for _, a := range addresses {
        if a != "" {
            addrSet[a] = struct{}{}
        }
    }

    // Check that at least one payment output goes to a watched address with sufficient amount
    hasSufficientPayment := false
    for _, vout := range tx.TxOut {
        // Skip provably-unspendable OP_RETURN outputs
        if txscript.GetScriptClass(vout.PkScript) == txscript.NullDataTy {
            continue
        }
        recv, err := ExtractAddressFromScript(vout.PkScript, network)
        if err != nil || recv == "" {
            continue
        }
        if _, ok := addrSet[recv]; ok && vout.Value >= minDepositAmount {
            hasSufficientPayment = true
            break
        }
    }
    if !hasSufficientPayment {
        return false, "", nil
    }

    // Find OP_RETURN and parse data payload
    var opretData []byte
    for _, vout := range tx.TxOut {
        if txscript.GetScriptClass(vout.PkScript) != txscript.NullDataTy {
            continue
        }
        data, ok := extractNullDataPayload(vout.PkScript)
        if ok {
            opretData = data
            break
        }
    }
    if len(opretData) == 0 {
        return false, "", nil
    }

    // Validate magic prefix and EVM address payload (20 bytes)
    if len(opretData) < len(magicBytes)+20 {
        return false, "", nil
    }
    if !bytes.Equal(opretData[:len(magicBytes)], magicBytes) {
        return false, "", nil
    }
    evmAddrBytes := opretData[len(magicBytes) : len(magicBytes)+20]
    evmAddr := "0x" + hex.EncodeToString(evmAddrBytes)
    return true, evmAddr, nil
}

// extractNullDataPayload attempts to extract the pushed data following OP_RETURN.
// It supports canonical small pushdata forms used by NullDataScript.
func extractNullDataPayload(script []byte) ([]byte, bool) {
    if len(script) == 0 || script[0] != txscript.OP_RETURN {
        return nil, false
    }
    // No data scenario: single OP_RETURN
    if len(script) == 1 {
        return []byte{}, true
    }
    // Parse push op
    i := 1
    if i >= len(script) {
        return nil, false
    }
    op := script[i]
    i++
    var dataLen int
    switch op {
    case 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
        0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
        0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
        0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29,
        0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
        0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f,
        0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49,
        0x4a, 0x4b: // direct small push (1..75)
        dataLen = int(op)
        if i+dataLen > len(script) {
            return nil, false
        }
        return script[i : i+dataLen], true
    case txscript.OP_PUSHDATA1:
        if i >= len(script) {
            return nil, false
        }
        dataLen = int(script[i])
        i++
        if i+dataLen > len(script) {
            return nil, false
        }
        return script[i : i+dataLen], true
    case txscript.OP_PUSHDATA2:
        if i+1 >= len(script) {
            return nil, false
        }
        dataLen = int(script[i]) | int(script[i+1])<<8
        i += 2
        if i+dataLen > len(script) {
            return nil, false
        }
        return script[i : i+dataLen], true
    default:
        return nil, false
    }
}

// VerifyBlockSPV verifies block SPV proof
func VerifyBlockSPV(block DogeBlockExt) error {
	// This is a simplified implementation
	// In a real implementation, you would verify the SPV proof
	return nil
}

// DecodeBase64Pubkey decodes a base64 encoded public key
func DecodeBase64Pubkey(pubkeyBase64 string) ([]byte, error) {
	pubkeyBytes, err := base64.StdEncoding.DecodeString(pubkeyBase64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 pubkey: %w", err)
	}
	return pubkeyBytes, nil
}

// ExtractAddressFromScript extracts address from a script
func ExtractAddressFromScript(script []byte, network *chaincfg.Params) (string, error) {
	_, addresses, _, err := txscript.ExtractPkScriptAddrs(script, network)
	if err != nil {
		return "", fmt.Errorf("failed to extract addresses from script: %w", err)
	}

	if len(addresses) == 0 {
		return "", fmt.Errorf("no address found in script")
	}

	return addresses[0].String(), nil
}

// IsP2PKHScript checks if a script is a P2PKH script
func IsP2PKHScript(script []byte) bool {
	return len(script) == 25 && script[0] == txscript.OP_DUP && script[1] == txscript.OP_HASH160 && script[2] == 20 && script[23] == txscript.OP_EQUALVERIFY && script[24] == txscript.OP_CHECKSIG
}

// IsP2WPKHScript checks if a script is a P2WPKH script
func IsP2WPKHScript(script []byte) bool {
	return len(script) == 22 && script[0] == txscript.OP_0 && script[1] == 20
}
