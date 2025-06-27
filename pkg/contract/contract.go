package contract

import (
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
)

type Contract struct {
	address  common.Address
	abi      abi.ABI
	contract *bind.BoundContract
}

// Global ABI types for reuse across functions
var (
	AddressArrayType abi.Type
	BytesArrayType   abi.Type
	BytesType        abi.Type
	Uint256Type      abi.Type
	AddressType      abi.Type
	BoolType         abi.Type
)

// Initialize ABI types
func init() {
	var err error

	AddressArrayType, err = abi.NewType("address[]", "", nil)
	if err != nil {
		panic("Failed to create address[] type: " + err.Error())
	}

	BytesArrayType, err = abi.NewType("bytes[]", "", nil)
	if err != nil {
		panic("Failed to create bytes[] type: " + err.Error())
	}

	BytesType, err = abi.NewType("bytes", "", nil)
	if err != nil {
		panic("Failed to create bytes type: " + err.Error())
	}

	Uint256Type, err = abi.NewType("uint256", "", nil)
	if err != nil {
		panic("Failed to create uint256 type: " + err.Error())
	}

	AddressType, err = abi.NewType("address", "", nil)
	if err != nil {
		panic("Failed to create address type: " + err.Error())
	}

	BoolType, err = abi.NewType("bool", "", nil)
	if err != nil {
		panic("Failed to create bool type: " + err.Error())
	}
}

// Helper functions using global ABI types for custom encoding

// EncodeAddressArray encodes an array of addresses using the global ABI type
func EncodeAddressArray(addresses []common.Address) ([]byte, error) {
	arguments := abi.Arguments{{Type: AddressArrayType}}
	return arguments.Pack(addresses)
}

// EncodeBytesArray encodes an array of bytes using the global ABI type
func EncodeBytesArray(data [][]byte) ([]byte, error) {
	arguments := abi.Arguments{{Type: BytesArrayType}}
	return arguments.Pack(data)
}

// EncodeUint256 encodes a big.Int as uint256 using the global ABI type
func EncodeUint256(value *big.Int) ([]byte, error) {
	arguments := abi.Arguments{{Type: Uint256Type}}
	return arguments.Pack(value)
}

// EncodeAddress encodes an address using the global ABI type
func EncodeAddress(addr common.Address) ([]byte, error) {
	arguments := abi.Arguments{{Type: AddressType}}
	return arguments.Pack(addr)
}

// EncodeBytes encodes bytes using the global ABI type
func EncodeBytes(data []byte) ([]byte, error) {
	arguments := abi.Arguments{{Type: BytesType}}
	return arguments.Pack(data)
}
