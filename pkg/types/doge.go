package types

import (
	"bytes"
	"encoding/base64"
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
	// This is a simplified implementation
	// In a real implementation, you would check for specific OP_RETURN data
	// that indicates a deposit transaction

	// For now, return false (not a deposit)
	return false, "", nil
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
