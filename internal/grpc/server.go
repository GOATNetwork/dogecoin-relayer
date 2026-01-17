package grpc

import (
	"context"
	"fmt"

	"github.com/goat-network/dogecoin-relayer/internal/doge"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type grpcServer struct {
	proto.UnimplementedBitcoinLightWalletServer
	conn       *models.DBConnection
	logger     *log.Entry
	dogeClient *doge.DogeClient
}

func newGRPCServer(conn *models.DBConnection, logger *log.Entry) *grpcServer {
	return &grpcServer{
		conn:   conn,
		logger: logger.WithField("component", "grpc-server"),
	}
}

func (s *grpcServer) setDogeClient(client *doge.DogeClient) {
	s.dogeClient = client
}

func (s *grpcServer) NewTransaction(ctx context.Context, req *proto.NewTransactionRequest) (*proto.NewTransactionResponse, error) {
	s.logger.Infof("Received NewTransaction request: txId=%s, evmAddr=%s", req.TransactionId, req.EvmAddress)

	if req.TransactionId == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_id is required")
	}
	if req.EvmAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "evm_address is required")
	}

	if len(req.TransactionId) != 64 {
		return nil, status.Error(codes.InvalidArgument, "transaction_id must be 64 characters (256-bit hash)")
	}

	if s.dogeClient == nil {
		cfg := global.GetConfig()
		if cfg == nil {
			return nil, status.Error(codes.FailedPrecondition, "configuration not initialized")
		}
		client, err := doge.NewDogeClient(cfg.Doge)
		if err != nil {
			s.logger.Errorf("Failed to create Doge client: %v", err)
			return nil, status.Error(codes.Internal, "failed to connect to Dogecoin network")
		}
		s.dogeClient = client
	}

	verifier, err := newDepositVerifier(s.dogeClient, s.conn, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create deposit verifier: %v", err)
		return nil, status.Error(codes.Internal, "failed to initialize deposit verifier")
	}

	evmAddresses, err := splitEvmAddresses(req.EvmAddress)
	if err != nil {
		s.logger.Errorf("Failed to parse EVM addresses: %v", err)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var lastErr error
	successCount := 0
	for _, evmAddr := range evmAddresses {
		result, err := verifier.verifyDeposit(ctx, req.TransactionId, req.RawTransaction, evmAddr)
		if err != nil {
			s.logger.Warnf("Deposit verification failed for evmAddr=%s: %v", evmAddr, err)
			lastErr = err
			continue
		}

		if err := verifier.saveDeposit(ctx, req.TransactionId, result); err != nil {
			s.logger.Errorf("Failed to save deposit for evmAddr=%s: %v", evmAddr, err)
			lastErr = err
			continue
		}

		successCount++
		s.logger.Infof("Deposit verified and saved: txid=%s, evmAddr=%s, amount=%d", req.TransactionId, result.EvmAddr, result.Amount)
	}

	if successCount == 0 && lastErr != nil {
		return nil, status.Error(codes.InvalidArgument, lastErr.Error())
	}

	return &proto.NewTransactionResponse{
		ErrorMessage: "",
	}, nil
}

func (s *grpcServer) QueryDepositAddress(ctx context.Context, req *proto.QueryDepositAddressRequest) (*proto.QueryDepositAddressResponse, error) {
	s.logger.Debug("Received QueryDepositAddress request")

	cfg := global.GetConfig()
	if cfg == nil {
		return nil, status.Error(codes.FailedPrecondition, "configuration not initialized")
	}

	if len(cfg.Doge.WatchAddresses) == 0 {
		s.logger.Warn("No deposit addresses configured")
		return nil, status.Error(codes.FailedPrecondition, "deposit address not configured")
	}

	depositAddress := cfg.Doge.WatchAddresses[0]
	s.logger.Debugf("Returning deposit address: %s", depositAddress)

	return &proto.QueryDepositAddressResponse{
		DepositAddress: depositAddress,
	}, nil
}

func loadServerTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
	}
	return creds, nil
}
