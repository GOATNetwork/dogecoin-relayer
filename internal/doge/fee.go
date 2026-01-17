package doge

import (
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
)

const (
	// MinFeeRateSatPerByte is the minimum fee rate for Dogecoin (0.001 DOGE/KB = 100 sat/byte)
	MinFeeRateSatPerByte int64 = 100
	// DefaultFeeRateSatPerByte is the default fee rate when network fee cannot be determined
	DefaultFeeRateSatPerByte int64 = 150
	// FeeRateSafetyMultiplier is the safety multiplier for recommended fee
	FeeRateSafetyMultiplier float64 = 1.5
)

// NetworkFee represents the network fee information
type NetworkFee struct {
	RelayFee       int64 // Minimum relay fee in sat/byte
	RecommendedFee int64 // Recommended fee in sat/byte (relay * safety)
	Source         string // Where the fee was obtained from
}

// GetNetworkFee fetches the current network fee rate
// Priority: estimatesmartfee -> getnetworkinfo.relayfee -> default
func (c *DogeClient) GetNetworkFee() (*NetworkFee, error) {
	// 1. Try estimatesmartfee (may not work on testnet)
	if fee, err := c.tryEstimateSmartFee(); err == nil && fee != nil {
		return fee, nil
	}

	// 2. Try getnetworkinfo.relayfee
	if fee, err := c.tryGetRelayFee(); err == nil && fee != nil {
		return fee, nil
	}

	// 3. Return default values
	c.logger.Warn("Could not get network fee from RPC, using default values")
	return &NetworkFee{
		RelayFee:       MinFeeRateSatPerByte,
		RecommendedFee: DefaultFeeRateSatPerByte,
		Source:         "default",
	}, nil
}

// tryEstimateSmartFee attempts to get fee rate from estimatesmartfee RPC
func (c *DogeClient) tryEstimateSmartFee() (*NetworkFee, error) {
	result, err := c.client.RawRequest("estimatesmartfee", []json.RawMessage{
		json.RawMessage("6"), // 6 blocks target
	})
	if err != nil {
		return nil, fmt.Errorf("estimatesmartfee RPC error: %w", err)
	}

	var resp struct {
		FeeRate float64 `json:"feerate"` // DOGE/KB
		Blocks  int     `json:"blocks"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal estimatesmartfee: %w", err)
	}

	// feerate == -1 means estimation failed (not enough data)
	if resp.FeeRate <= 0 {
		return nil, fmt.Errorf("estimatesmartfee returned invalid rate: %f", resp.FeeRate)
	}

	// Convert DOGE/KB to sat/byte
	// 1 DOGE = 100,000,000 satoshis
	// 1 KB = 1000 bytes
	// DOGE/KB * 100,000,000 / 1000 = sat/byte
	satPerByte := int64(resp.FeeRate * 100000)
	if satPerByte < MinFeeRateSatPerByte {
		satPerByte = MinFeeRateSatPerByte
	}

	recommended := int64(float64(satPerByte) * FeeRateSafetyMultiplier)
	c.logger.Infof("Got fee rate from estimatesmartfee: %d sat/byte (recommended: %d)", satPerByte, recommended)

	return &NetworkFee{
		RelayFee:       satPerByte,
		RecommendedFee: recommended,
		Source:         "estimatesmartfee",
	}, nil
}

// tryGetRelayFee attempts to get minimum relay fee from getnetworkinfo RPC
func (c *DogeClient) tryGetRelayFee() (*NetworkFee, error) {
	result, err := c.client.RawRequest("getnetworkinfo", nil)
	if err != nil {
		return nil, fmt.Errorf("getnetworkinfo RPC error: %w", err)
	}

	var resp struct {
		RelayFee       float64 `json:"relayfee"`       // DOGE/KB
		IncrementalFee float64 `json:"incrementalfee"` // DOGE/KB
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal getnetworkinfo: %w", err)
	}

	if resp.RelayFee <= 0 {
		return nil, fmt.Errorf("getnetworkinfo returned invalid relayfee: %f", resp.RelayFee)
	}

	// Convert DOGE/KB to sat/byte
	satPerByte := int64(resp.RelayFee * 100000)
	if satPerByte < MinFeeRateSatPerByte {
		satPerByte = MinFeeRateSatPerByte
	}

	recommended := int64(float64(satPerByte) * FeeRateSafetyMultiplier)
	c.logger.Infof("Got fee rate from getnetworkinfo: %d sat/byte (recommended: %d)", satPerByte, recommended)

	return &NetworkFee{
		RelayFee:       satPerByte,
		RecommendedFee: recommended,
		Source:         "getnetworkinfo",
	}, nil
}

// GetEffectiveFeeRate returns the effective fee rate to use for transactions
// It tries to get the network fee, falls back to config, then to default
func GetEffectiveFeeRate(client *DogeClient, configFeeRate int64) int64 {
	logger := log.WithField("module", "fee")

	// 1. Try to get from network
	if client != nil {
		if networkFee, err := client.GetNetworkFee(); err == nil {
			logger.Infof("Using network fee rate: %d sat/byte (source: %s)",
				networkFee.RecommendedFee, networkFee.Source)
			return networkFee.RecommendedFee
		}
	}

	// 2. Use config value if >= minimum
	if configFeeRate >= MinFeeRateSatPerByte {
		logger.Infof("Using config fee rate: %d sat/byte", configFeeRate)
		return configFeeRate
	}

	// 3. Use default
	logger.Warnf("Config fee rate %d is below minimum %d, using default %d sat/byte",
		configFeeRate, MinFeeRateSatPerByte, DefaultFeeRateSatPerByte)
	return DefaultFeeRateSatPerByte
}
