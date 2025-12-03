package types

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"slices"
	"strings"

	log "github.com/sirupsen/logrus"
)

// DecodeDogecoinHash decodes a transaction or block hash from big-endian hex string to little-endian byte slice
func DecodeDogecoinHash(hash string) ([]byte, error) {
	data, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	txid := slices.Clone(data)
	slices.Reverse(txid)
	return txid, nil
}

// EncodeDogecoinHash encodes a transaction or block hash from little-endian byte slice to big-endian hex string
func EncodeDogecoinHash(hash []byte) (string, error) {
	txid := slices.Clone(hash)
	slices.Reverse(txid)
	return hex.EncodeToString(txid), nil
}

func ParseScientificNotation(input string) (*big.Int, bool) {
	// check if contains 'e' or 'E'
	parts := strings.Split(strings.ToLower(input), "e")
	if len(parts) == 1 {
		// no scientific notation, convert directly
		return new(big.Int).SetString(parts[0], 10)
	} else if len(parts) != 2 {
		return nil, false
	}

	// parse base and exponent
	baseStr, expStr := parts[0], parts[1]

	// convert base and exponent to big.Int
	base := new(big.Float)
	if _, ok := base.SetString(baseStr); !ok {
		return nil, false
	}

	exp := new(big.Int)
	if _, ok := exp.SetString(expStr, 10); !ok {
		return nil, false
	}

	// calculate result: base * (10 ^ exp)
	scale := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), exp, nil))
	resultFloat := new(big.Float).Mul(base, scale)

	// convert to *big.Int
	result := new(big.Int)
	resultFloat.Int(result) // discard decimal part
	return result, true
}

// InitLogEntry initializes a log entry with the given module name
func InitLogEntry(module string) *log.Entry {
	logger := log.New().WithField("Module", module)
	logger.Logger.SetLevel(log.GetLevel())
	logger.Logger.SetOutput(os.Stdout)
	return logger
}

func CalculateBigInt(a, b string, isAddition bool) (*big.Int, error) {
	bigA, ok := new(big.Int).SetString(a, 10)
	if !ok {
		return nil, fmt.Errorf("invalid big.Int string: %s", a)
	}
	bigB, ok := new(big.Int).SetString(b, 10)
	if !ok {
		return nil, fmt.Errorf("invalid big.Int string: %s", b)
	}
	var result *big.Int
	if isAddition {
		result = new(big.Int).Add(bigA, bigB)
	} else {
		result = new(big.Int).Sub(bigA, bigB)
	}
	return result, nil
}
