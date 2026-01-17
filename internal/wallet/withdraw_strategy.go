package wallet

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"

	"github.com/dogecoinw/doged/btcec"
	"github.com/dogecoinw/doged/btcec/ecdsa"
	"github.com/dogecoinw/doged/btcutil"
	"github.com/dogecoinw/doged/chaincfg"
	"github.com/dogecoinw/doged/chaincfg/chainhash"
	"github.com/dogecoinw/doged/txscript"
	"github.com/dogecoinw/doged/wire"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	log "github.com/sirupsen/logrus"
)

type FireblocksSignedMessage struct {
	Content   string              `json:"content"`
	Signature FireblocksSignature `json:"signature"`
	PublicKey string              `json:"publicKey"`
}

type FireblocksSignature struct {
	R string `json:"r"`
	S string `json:"s"`
}

func GetDustAmount(txPrice int64) int64 {
	return txPrice * 31 * 3
}

func TransactionSizeEstimateV2(numInputs int, receiverTypes []string, numOutputs int, utxoTypes []string) (float64, int64) {
	baseSize := int64(4 + 4)
	witnessSize := int64(0)

	baseSize += 1

	for _, utxoType := range utxoTypes {
		switch utxoType {
		case WALLET_TYPE_P2WPKH:
			baseSize += 41
			witnessSize += 108
		case WALLET_TYPE_P2PKH:
			baseSize += 148
		case WALLET_TYPE_P2WSH:
			baseSize += 41
			witnessSize += 132
		default:
			baseSize += 41
			witnessSize += 108
		}
	}

	baseSize += 1
	for _, receiverType := range receiverTypes {
		switch receiverType {
		case WALLET_TYPE_P2PKH:
			baseSize += 34
		case WALLET_TYPE_P2WPKH:
			baseSize += 31
		case WALLET_TYPE_P2WSH:
			baseSize += 43
		default:
			baseSize += 31
		}
	}

	if len(receiverTypes) < numOutputs {
		baseSize += int64(31 * (numOutputs - len(receiverTypes)))
	}

	if witnessSize > 0 {
		witnessSize += 2
	}

	weight := baseSize*4 + witnessSize
	virtualSize := float64(weight) / float64(4)

	return virtualSize, witnessSize
}

func ConsolidateSmallUTXOs(utxos []*UTXOAdapter, networkFee, threshold int64, maxVin, trigerNum int) ([]*UTXOAdapter, int64, int64, error) {
	maxFee := int64(100000)
	cfg := global.GetConfig()
	if cfg != nil && cfg.Withdraw.FeeRate > 0 {
		maxFee = cfg.Withdraw.FeeRate * 1000
	}
	if networkFee > maxFee {
		return nil, 0, 0, fmt.Errorf("network fee is too high, cannot consolidate")
	}
	if trigerNum < 10 {
		trigerNum = 500
	}
	if len(utxos) < trigerNum {
		return nil, 0, 0, fmt.Errorf("not enough utxos to consolidate")
	}

	var smallUTXOs []*UTXOAdapter
	var totalAmount int64 = 0

	for _, utxo := range utxos {
		if utxo.ReceiverType != WALLET_TYPE_P2WPKH && utxo.ReceiverType != WALLET_TYPE_P2WSH && utxo.ReceiverType != WALLET_TYPE_P2PKH {
			continue
		}
		if utxo.Amount < threshold {
			smallUTXOs = append(smallUTXOs, utxo)
			totalAmount += utxo.Amount
		}
		if len(smallUTXOs) >= maxVin {
			break
		}
	}

	if len(smallUTXOs) < trigerNum {
		return nil, 0, 0, fmt.Errorf("not enough small utxos to consolidate")
	}

	utxoTypes := make([]string, len(smallUTXOs))
	for i, utxo := range smallUTXOs {
		utxoTypes[i] = utxo.ReceiverType
	}

	txSize, _ := TransactionSizeEstimateV2(len(smallUTXOs), []string{WALLET_TYPE_P2WPKH}, 1, utxoTypes)
	estimatedFee := int64(math.Ceil(txSize)) * networkFee

	dustThreshold := GetDustAmount(networkFee)
	if totalAmount < dustThreshold {
		return nil, 0, 0, fmt.Errorf("total amount is too low, cannot consolidate")
	}

	finalAmount := totalAmount - estimatedFee
	if finalAmount <= 0 {
		return nil, 0, 0, fmt.Errorf("consolidation fee is too high, cannot consolidate")
	}

	return smallUTXOs, totalAmount, finalAmount, nil
}

func SelectOptimalUTXOs(utxos []*UTXOAdapter, receiverTypes []string, withdrawAmount, externalAmount, networkFee int64, withdrawTotal int) ([]*UTXOAdapter, int64, int64, int64, float64, int64, error) {
	var selectedUTXOs []*UTXOAdapter
	var totalSelectedAmount int64 = 0
	var witnessSize int64 = 0
	var txSize float64 = 0
	var maxVin int = 10

	requiredAmount := withdrawAmount + externalAmount
	if requiredAmount > 50*1e8 {
		maxVin = 50
	}

	utxoTypes := make([]string, 0)
	txSize, witnessSize = TransactionSizeEstimateV2(len(utxoTypes), receiverTypes, withdrawTotal, utxoTypes)
	estimatedFee := txSize * float64(networkFee)

	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].Amount < utxos[j].Amount
	})

	found := false

	for i := 0; i < len(utxos); i++ {
		if utxos[i].Amount >= requiredAmount {
			selectedUTXOs = []*UTXOAdapter{utxos[i]}
			totalSelectedAmount = utxos[i].Amount
			utxoTypes = []string{utxos[i].ReceiverType}
			found = true
			log.Debugf("SelectOptimalUTXOs found single utxo matched: %s-%d, amount: %d", utxos[i].Txid, utxos[i].OutIndex, utxos[i].Amount)
			break
		}
	}

	if !found {
		for i := 1; i < len(utxos); i++ {
			if utxos[i-1].Amount+utxos[i].Amount >= requiredAmount {
				selectedUTXOs = []*UTXOAdapter{utxos[i-1], utxos[i]}
				totalSelectedAmount = utxos[i-1].Amount + utxos[i].Amount
				utxoTypes = []string{utxos[i-1].ReceiverType, utxos[i].ReceiverType}
				found = true
				log.Debugf("SelectOptimalUTXOs found two utxos matched: %s-%d, %s-%d, amount: %d", utxos[i-1].Txid, utxos[i-1].OutIndex, utxos[i].Txid, utxos[i].OutIndex, utxos[i-1].Amount+utxos[i].Amount)
				break
			}
		}
	}

	if found {
		txSize, witnessSize = TransactionSizeEstimateV2(len(selectedUTXOs), receiverTypes, withdrawTotal, utxoTypes)
		estimatedFee = txSize * float64(networkFee)
	} else {
		sort.Slice(utxos, func(i, j int) bool {
			return utxos[i].Amount > utxos[j].Amount
		})

		for _, utxo := range utxos {
			if totalSelectedAmount >= requiredAmount {
				break
			}
			selectedUTXOs = append(selectedUTXOs, utxo)
			totalSelectedAmount += utxo.Amount

			utxoTypes = append(utxoTypes, utxo.ReceiverType)
			txSize, witnessSize = TransactionSizeEstimateV2(len(selectedUTXOs), receiverTypes, withdrawTotal, utxoTypes)
			estimatedFee = txSize * float64(networkFee)

			if len(selectedUTXOs) >= maxVin {
				break
			}
		}
	}

	var smallestUTXO *UTXOAdapter
	for _, utxo := range utxos {
		isAlreadySelected := false
		for _, selected := range selectedUTXOs {
			if selected.Txid == utxo.Txid && selected.OutIndex == utxo.OutIndex {
				isAlreadySelected = true
				break
			}
		}
		if isAlreadySelected {
			continue
		}

		if utxo.Amount > networkFee*(296+31) && (smallestUTXO == nil || utxo.Amount < smallestUTXO.Amount) {
			smallestUTXO = utxo
		}
	}

	if smallestUTXO != nil && smallestUTXO.Amount > 0 &&
		smallestUTXO.Amount < SMALL_UTXO_DEFINE {
		selectedUTXOs = append(selectedUTXOs, smallestUTXO)
		totalSelectedAmount += smallestUTXO.Amount

		utxoTypes = append(utxoTypes, smallestUTXO.ReceiverType)
		txSize, witnessSize = TransactionSizeEstimateV2(len(selectedUTXOs), receiverTypes, withdrawTotal, utxoTypes)
		estimatedFee = txSize * float64(networkFee)
	}

	if totalSelectedAmount < requiredAmount {
		return nil, 0, 0, 0, estimatedFee, 0, fmt.Errorf("not enough utxos to satisfy the withdrawal amount and network fee, withdraw amount: %d, selected amount: %d, estimated fee: %f", withdrawAmount, totalSelectedAmount, estimatedFee)
	}

	changeAmount := totalSelectedAmount - withdrawAmount
	dustThreshold := GetDustAmount(networkFee)
	if changeAmount > dustThreshold {
		txSize, witnessSize = TransactionSizeEstimateV2(len(selectedUTXOs), receiverTypes, withdrawTotal+1, utxoTypes)
		estimatedFee = txSize * float64(networkFee)
	} else {
		estimatedFee += float64(changeAmount)
		changeAmount = 0
	}

	return selectedUTXOs, totalSelectedAmount, withdrawAmount, changeAmount, estimatedFee, witnessSize, nil
}

func CreateRawTransaction(params *TransactionParams) (*wire.MsgTx, uint64, error) {
	tx := wire.NewMsgTx(wire.TxVersion)

	for _, utxo := range params.UTXOs {
		hash, err := chainhash.NewHashFromStr(utxo.Txid)
		if err != nil {
			return nil, 0, &TransactionError{
				Code:    ErrInvalidUTXO,
				Message: fmt.Sprintf("invalid UTXO txid: %s", utxo.Txid),
				Err:     err,
			}
		}
		outPoint := wire.NewOutPoint(hash, uint32(utxo.OutIndex))
		txIn := wire.NewTxIn(outPoint, nil, nil)
		txIn.Sequence = 0xffffff01
		tx.AddTxIn(txIn)
	}

	actualFee := int64(0)
	changeFee := int64(0)

	if len(params.Withdrawals) > 0 {
		totalTxout := len(params.Withdrawals)
		actualFee = int64(math.Ceil(params.EstimatedFee / float64(totalTxout)))
	} else {
		changeFee = int64(math.Ceil(params.EstimatedFee))
	}

	for _, withdrawal := range params.Withdrawals {
		addr, err := btcutil.DecodeAddress(withdrawal.DestAddress, params.Net.Params)
		if err != nil {
			return nil, 0, &TransactionError{
				Code:    ErrInvalidAddress,
				Message: fmt.Sprintf("invalid withdrawal address: %s", withdrawal.DestAddress),
				Err:     err,
			}
		}
		pkScript, err := txscript.PayToAddrScript(addr)
		if err != nil {
			return nil, 0, &TransactionError{
				Code:    ErrInvalidScript,
				Message: "failed to create payment script",
				Err:     err,
			}
		}
		val := withdrawal.Amount - actualFee
		dustThreshold := GetDustAmount(params.NetworkFee)
		if val <= dustThreshold {
			return nil, 0, &TransactionError{
				Code:    ErrWithdrawDustAmount,
				Message: fmt.Sprintf("withdrawal amount too small after fee deduction: %d", val),
			}
		}
		tx.AddTxOut(wire.NewTxOut(val, pkScript))
	}

	val := int64(params.ChangeAmount) - changeFee
	if val > 0 {
		changeAddr, err := btcutil.DecodeAddress(params.ChangeAddress, params.Net.Params)
		if err != nil {
			return nil, 0, &TransactionError{
				Code:    ErrInvalidAddress,
				Message: fmt.Sprintf("invalid change address: %s", params.ChangeAddress),
				Err:     err,
			}
		}
		changePkScript, err := txscript.PayToAddrScript(changeAddr)
		if err != nil {
			return nil, 0, &TransactionError{
				Code:    ErrInvalidScript,
				Message: "failed to create change script",
				Err:     err,
			}
		}
		dustThreshold := GetDustAmount(params.NetworkFee)
		if val <= dustThreshold {
			return nil, 0, &TransactionError{
				Code:    ErrChangeDustAmount,
				Message: fmt.Sprintf("change amount too small after fee deduction: %d", val),
			}
		}
		tx.AddTxOut(wire.NewTxOut(val, changePkScript))
	}

	return tx, uint64(actualFee), nil
}

func GenerateRawMessageToFireblocks(tx *wire.MsgTx, utxos []*UTXOAdapter, net *chaincfg.Params) ([][]byte, error) {
	hashes := make([][]byte, len(utxos))

	for i, utxo := range utxos {
		var pkScript []byte

		switch utxo.ReceiverType {
		case WALLET_TYPE_P2PKH:
			addr, err := btcutil.DecodeAddress(utxo.Receiver, net)
			if err != nil {
				return nil, err
			}
			pkScript, err = txscript.PayToAddrScript(addr)
			if err != nil {
				return nil, err
			}

			hash, err := txscript.CalcSignatureHash(pkScript, txscript.SigHashAll, tx, i)
			if err != nil {
				return nil, err
			}
			hashes[i] = hash

		case WALLET_TYPE_P2WPKH:
			addr, err := btcutil.DecodeAddress(utxo.Receiver, net)
			if err != nil {
				return nil, err
			}
			pkScript, err = txscript.PayToAddrScript(addr)
			if err != nil {
				return nil, err
			}

			inputFetcher := txscript.NewCannedPrevOutputFetcher(pkScript, utxo.Amount)

			hash, err := txscript.CalcWitnessSigHash(pkScript, txscript.NewTxSigHashes(tx, inputFetcher), txscript.SigHashAll, tx, i, utxo.Amount)
			if err != nil {
				return nil, err
			}
			hashes[i] = hash

		case WALLET_TYPE_P2WSH:
			redeemScriptHash := sha256.Sum256(utxo.SubScript)
			prevPkScript, err := txscript.NewScriptBuilder().AddOp(txscript.OP_0).AddData(redeemScriptHash[:]).Script()
			if err != nil {
				return nil, err
			}

			inputFetcher := txscript.NewCannedPrevOutputFetcher(prevPkScript, utxo.Amount)

			hash, err := txscript.CalcWitnessSigHash(utxo.SubScript, txscript.NewTxSigHashes(tx, inputFetcher), txscript.SigHashAll, tx, i, utxo.Amount)
			if err != nil {
				return nil, err
			}
			hashes[i] = hash

		default:
			return nil, fmt.Errorf("unknown UTXO type: %s", utxo.ReceiverType)
		}
	}

	return hashes, nil
}

func FindFireblocksSignedMessage(rawHash []byte, fbSignedMessages []FireblocksSignedMessage) (*FireblocksSignedMessage, error) {
	for _, signedMessage := range fbSignedMessages {
		if signedMessage.Content == hex.EncodeToString(rawHash) {
			return &signedMessage, nil
		}
	}
	return nil, fmt.Errorf("signed message not found")
}

func ApplyFireblocksSignaturesToTx(tx *wire.MsgTx, utxos []*UTXOAdapter, fbSignedMessages []FireblocksSignedMessage, net *chaincfg.Params) error {
	if len(utxos) != len(fbSignedMessages) {
		return fmt.Errorf("number of UTXOs and signed messages do not match")
	}

	rawHashes, err := GenerateRawMessageToFireblocks(tx, utxos, net)
	if err != nil {
		return fmt.Errorf("error generating raw message to fireblocks: %v", err)
	}

	for i, utxo := range utxos {
		signedMessage, err := FindFireblocksSignedMessage(rawHashes[i], fbSignedMessages)
		if err != nil {
			return fmt.Errorf("error finding fireblocks signed message: %v", err)
		}

		derSignature, err := convertToDERSignature(signedMessage.Signature)
		if err != nil {
			return fmt.Errorf("error converting Fireblocks signature to DER: %v", err)
		}

		switch utxo.ReceiverType {
		case WALLET_TYPE_P2PKH:
			pubKeyBytes, err := hex.DecodeString(signedMessage.PublicKey)
			if err != nil {
				return fmt.Errorf("error decoding public key: %v", err)
			}

			sigScript, err := txscript.NewScriptBuilder().
				AddData(derSignature).
				AddData(pubKeyBytes).
				Script()
			if err != nil {
				return fmt.Errorf("error building signature script: %v", err)
			}

			tx.TxIn[i].SignatureScript = sigScript

		case WALLET_TYPE_P2WPKH:
			pubKeyBytes, err := hex.DecodeString(signedMessage.PublicKey)
			if err != nil {
				return fmt.Errorf("error decoding public key: %v", err)
			}

			tx.TxIn[i].Witness = wire.TxWitness{
				derSignature,
				pubKeyBytes,
			}

		case WALLET_TYPE_P2WSH:
			tx.TxIn[i].Witness = wire.TxWitness{
				derSignature,
				utxo.SubScript,
			}

		default:
			return fmt.Errorf("unknown UTXO type: %s", utxo.ReceiverType)
		}
	}

	var buf bytes.Buffer
	if err := tx.Serialize(&buf); err != nil {
		log.Errorf("error serializing transaction %s: %v", tx.TxHash(), err)
	} else {
		log.Debugf("Serialized transaction: %s, raw hex: %x", tx.TxHash(), buf.Bytes())
	}
	return nil
}

func convertToDERSignature(fbSig FireblocksSignature) ([]byte, error) {
	rBytes, err := hex.DecodeString(fbSig.R)
	if err != nil {
		return nil, fmt.Errorf("error decoding R: %v", err)
	}

	if len(rBytes) > 32 {
		return nil, fmt.Errorf("R is too long")
	}

	if len(rBytes) < 32 {
		paddedR := make([]byte, 32)
		copy(paddedR[32-len(rBytes):], rBytes)
		rBytes = paddedR
	}

	sBytes, err := hex.DecodeString(fbSig.S)
	if err != nil {
		return nil, fmt.Errorf("error decoding S: %v", err)
	}

	if len(sBytes) > 32 {
		return nil, fmt.Errorf("S is too long")
	}

	if len(sBytes) < 32 {
		paddedS := make([]byte, 32)
		copy(paddedS[32-len(sBytes):], sBytes)
		sBytes = paddedS
	}

	var rMod btcec.ModNScalar
	var sMod btcec.ModNScalar
	rMod.SetByteSlice(rBytes)
	sMod.SetByteSlice(sBytes)

	signature := ecdsa.NewSignature(&rMod, &sMod)

	derSig := signature.Serialize()

	return append(derSig, byte(txscript.SigHashAll)), nil
}

func SignTransactionByPrivKey(privKey *btcec.PrivateKey, tx *wire.MsgTx, utxos []*UTXOAdapter, net *chaincfg.Params) error {
	for i, utxo := range utxos {
		switch utxo.ReceiverType {
		case WALLET_TYPE_P2PKH:
			addr, err := btcutil.DecodeAddress(utxo.Receiver, net)
			if err != nil {
				return err
			}
			pkScript, err := txscript.PayToAddrScript(addr)
			if err != nil {
				return err
			}

			sigScript, err := txscript.SignatureScript(tx, i, pkScript, txscript.SigHashAll, privKey, true)
			if err != nil {
				return err
			}

			tx.TxIn[i].SignatureScript = sigScript

		case WALLET_TYPE_P2WPKH:
			addr, err := btcutil.DecodeAddress(utxo.Receiver, net)
			if err != nil {
				return err
			}
			pkScript, err := txscript.PayToAddrScript(addr)
			if err != nil {
				return err
			}

			witnessSig, err := txscript.RawTxInWitnessSignature(tx, txscript.NewTxSigHashes(tx, txscript.NewCannedPrevOutputFetcher(pkScript, utxo.Amount)), i, utxo.Amount, pkScript, txscript.SigHashAll, privKey)
			if err != nil {
				return err
			}

			tx.TxIn[i].Witness = wire.TxWitness{
				witnessSig,
				privKey.PubKey().SerializeCompressed(),
			}

		case WALLET_TYPE_P2WSH:
			redeemScriptHash := sha256.Sum256(utxo.SubScript)
			prevPkScript, err := txscript.NewScriptBuilder().AddOp(txscript.OP_0).AddData(redeemScriptHash[:]).Script()
			if err != nil {
				return err
			}

			inputFetcher := txscript.NewCannedPrevOutputFetcher(prevPkScript, utxo.Amount)

			witnessSig, err := txscript.RawTxInWitnessSignature(tx, txscript.NewTxSigHashes(tx, inputFetcher), i, utxo.Amount, utxo.SubScript, txscript.SigHashAll, privKey)
			if err != nil {
				return err
			}

			tx.TxIn[i].Witness = wire.TxWitness{
				witnessSig,
				utxo.SubScript,
			}

		default:
			return fmt.Errorf("unknown UTXO type: %s", utxo.ReceiverType)
		}
	}

	return nil
}
