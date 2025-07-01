package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/types"

	log "github.com/sirupsen/logrus"
)

// Protocol constants
const (
	ProtocolID   = "/goat-network/dogecoin-relayer/1.0.0"
	DiscoveryTag = "goat-dogecoin-relayer"
	LibP2PTopic  = "goat-dogecoin-relayer-protocol"
)

// Message represents a P2P message
type Message struct {
	// Type is the message type
	Type types.P2PMessageType
	// ID is a unique message ID
	ID string
	// SessionID is the ID of the session this message belongs to
	SessionID string
	// From is the sender's peer ID
	From peer.ID
	// To is the recipient's peer ID
	To *peer.ID
	// Payload is the message payload
	Payload []byte
	// Timestamp is the message timestamp
	Timestamp int64
	// Signature is the message signature, used for message verification
	Signature []byte
}

// Network provides P2P networking functionality
// Network struct remains largely the same

type Network struct {
	config   config.P2PConfig
	logger   *log.Entry
	host     host.Host
	eventBus *eventbus.Bus
	handlers map[types.P2PMessageType]func(*types.P2PBroadcastMessage) error
	ps       *pubsub.PubSub
	topic    *pubsub.Topic
	ctx      context.Context
	cancel   context.CancelFunc
}

func StringToPeerID(peerIDStr string) (peer.ID, error) {
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return "", fmt.Errorf("failed to decode peer ID: %w", err)
	}
	return peerID, nil
}

// displayPublicKey prints the node's public key in hex format and PeerID
func displayPublicKey(host host.Host) {
	pub := host.Peerstore().PubKey(host.ID())
	if pub == nil {
		log.Errorf("public key not found in peerstore")
		return
	}
	raw, err := crypto.MarshalPublicKey(pub)
	if err != nil {
		log.Errorf("marshal public key error: %v", err)
		return
	}
	hexKey := hex.EncodeToString(raw)
	log.Debugf("Node PeerID: %s", host.ID().String())
	log.Debugf("Public Key (secp256k1) hex: %s", hexKey)
}

func loadEthKey(hexkey string) (crypto.PrivKey, error) {
	b, err := hex.DecodeString(hexkey)
	if err != nil {
		return nil, fmt.Errorf("invalid hex key: %w", err)
	}
	if len(b) != 32 {
		return nil, fmt.Errorf("expected 32 bytes, got %d", len(b))
	}
	priv, err := crypto.UnmarshalSecp256k1PrivateKey(b)
	if err != nil {
		return nil, fmt.Errorf("unmarshal secp256k1: %w", err)
	}
	return priv, nil
}

func convertLibP2pPubKeyToEthereumAddress(pub crypto.PubKey) (common.Address, error) {
	// get libp2p pubkey raw bytes
	pubBytes, err := pub.Raw()
	if err != nil {
		return common.Address{}, fmt.Errorf("get raw pubkey: %w", err)
	}

	// libp2p secp256k1 pubkey format:
	// compressed format: 33 bytes (0x02/0x03 + 32 bytes)
	// uncompressed format: 65 bytes (0x04 + 32 bytes X + 32 bytes Y)

	var uncompressedPubKey []byte

	if len(pubBytes) == 33 {
		// compressed format, need to convert to ethereum format
		pubKeyECDSA, err := ethcrypto.DecompressPubkey(pubBytes)
		if err != nil {
			return common.Address{}, fmt.Errorf("decompress pubkey: %w", err)
		}

		// convert ECDSA pubkey to bytes format (65 bytes: 0x04 + X + Y)
		uncompressedPubKey = ethcrypto.FromECDSAPub(pubKeyECDSA)
	} else if len(pubBytes) == 65 && pubBytes[0] == 0x04 {
		// already uncompressed format
		uncompressedPubKey = pubBytes
	} else {
		return common.Address{}, fmt.Errorf("unexpected pubkey format, length: %d, first byte: 0x%02x", len(pubBytes), pubBytes[0])
	}

	// ethereum address calculation: Keccak256(X+Y) last 20 bytes
	// remove 0x04 prefix, only keep 64 bytes of X+Y coordinates
	if len(uncompressedPubKey) != 65 || uncompressedPubKey[0] != 0x04 {
		return common.Address{}, fmt.Errorf("invalid uncompressed pubkey format: length=%d, first_byte=0x%02x", len(uncompressedPubKey), uncompressedPubKey[0])
	}

	hash := ethcrypto.Keccak256(uncompressedPubKey[1:]) // remove 0x04 prefix
	var addr common.Address
	copy(addr[:], hash[12:]) // last 20 bytes
	return addr, nil
}

// NewNetwork creates and initializes a new P2P network
func NewNetwork(ctx context.Context, config config.P2PConfig) (*Network, error) {
	logger := types.InitLogEntry("p2p-network")
	priv, err := loadEthKey(config.ProposerPrivateKey)
	if err != nil {
		return nil, err
	}
	options := []libp2p.Option{
		libp2p.Identity(priv),
	}
	if config.ListenAddr != "" {
		addr, err := multiaddr.NewMultiaddr(config.ListenAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid listen address: %w", err)
		}
		options = append(options, libp2p.ListenAddrs(addr))
	}

	addrsOpt := libp2p.AddrsFactory(func(in []multiaddr.Multiaddr) (out []multiaddr.Multiaddr) {
		for _, a := range in {
			if manet.IsPublicAddr(a) || manet.IsPrivateAddr(a) {
				// exclude 0.0.0.0 / 127.0.0.1 / link-local
				if !manet.IsIPLoopback(a) && !manet.IsIP6LinkLocal(a) && !manet.IsIPUnspecified(a) {
					out = append(out, a)
				}
			}
		}
		for _, s := range strings.FieldsFunc(config.ExternalAddr, func(r rune) bool { return r == ',' || r == ' ' }) {
			if m, err := multiaddr.NewMultiaddr(strings.TrimSpace(s)); err == nil {
				out = append(out, m)
			} else {
				logger.Warnf("bad multiaddr %q: %v", s, err)
			}
		}
		return
	})
	options = append(options, addrsOpt)

	host, err := libp2p.New(options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	// display pubkey
	displayPublicKey(host)

	if config.KeyDir != "" {
		idPath := filepath.Join(config.KeyDir, "libp2p.id")
		if _, err := os.Stat(idPath); os.IsNotExist(err) {
			id := host.ID().String()
			if err := os.WriteFile(idPath, []byte(id), 0600); err != nil {
				return nil, fmt.Errorf("failed to write libp2p id: %w", err)
			}
		}
	}

	ps, err := pubsub.NewGossipSub(ctx, host,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
		pubsub.WithPeerOutboundQueueSize(1000),
		pubsub.WithPeerExchange(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub: %w", err)
	}

	topic, err := ps.Join(LibP2PTopic)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic %s: %w", LibP2PTopic, err)
	}

	ctx, cancel := context.WithCancel(ctx)

	n := &Network{
		config:   config,
		logger:   logger,
		host:     host,
		eventBus: global.GetEventBus(),
		handlers: make(map[types.P2PMessageType]func(*types.P2PBroadcastMessage) error),
		ps:       ps,
		topic:    topic,
		ctx:      ctx,
		cancel:   cancel,
	}

	n.host.SetStreamHandler(protocol.ID(ProtocolID), n.handleStream)

	go n.handlePubSubMessages()
	go n.startHeartbeat()

	n.eventBus.Publish(eventbus.EventNetworkInitialized, n.host.ID())
	logger.Infof("P2P network initialized with PubSub. Node ID: %s", n.host.ID())
	for _, addr := range n.host.Addrs() {
		logger.Infof("Listening on: %s/p2p/%s", addr, n.host.ID())
	}

	return n, nil
}

func (n *Network) checkWhitelisted(peerID peer.ID) error {
	// check if the peerID is in the whitelist
	pubKey := n.host.Peerstore().PubKey(peerID)
	if pubKey == nil {
		return fmt.Errorf("public key not found for peer %s", peerID)
	}

	ethAddr, err := convertLibP2pPubKeyToEthereumAddress(pubKey)
	if err != nil {
		return fmt.Errorf("convert pubkey to ethereum address: %w", err)
	}
	n.logger.Debugf("Peer %s public key converted to Ethereum address: %s", peerID, ethAddr.Hex())

	// TODO: check if the peerID is in the whitelist

	return nil
}

func (n *Network) handlePubSubMessages() {
	sub, err := n.topic.Subscribe()
	if err != nil {
		n.logger.Errorf("Failed to subscribe to topic %s: %v", LibP2PTopic, err)
		return
	}
	defer sub.Cancel()

	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		msg, err := sub.Next(n.ctx)
		if err != nil {
			// Check if context was cancelled
			select {
			case <-n.ctx.Done():
				return
			default:
				log.Errorf("Error receiving pubsub message: %v", err)
				continue
			}
		}

		if msg.GetFrom() == n.host.ID() {
			continue
		}

		var libp2pMsg Message
		if err := json.Unmarshal(msg.Data, &libp2pMsg); err != nil {
			n.logger.Errorf("Error unmarshaling pubsub message: %v", err)
			continue
		}

		if msg.GetFrom() != libp2pMsg.From {
			n.logger.Warnf("Message sender mismatch: expected %s, got %s", libp2pMsg.From, msg.GetFrom())
			continue
		}

		// Verify signature
		if err := n.checkWhitelisted(libp2pMsg.From); err != nil {
			n.logger.Errorf("Whitelisted check failed: %v", err)
			continue
		}

		if libp2pMsg.To != nil && *libp2pMsg.To != n.host.ID() {
			continue
		}

		n.logger.Debugf("Received pubsub message: Type=%s, ID=%s, From=%s, To=%s, SessionID=%s",
			libp2pMsg.Type, libp2pMsg.ID, libp2pMsg.From, libp2pMsg.To, libp2pMsg.SessionID)
		if libp2pMsg.Type == types.P2PMessageTypeHeartbeat {
			n.logger.Infof("💓 Received heartbeat from %s: %s", libp2pMsg.From, string(libp2pMsg.Payload))
		}

		handler, exists := n.handlers[libp2pMsg.Type]
		if exists {
			if err := handler(&types.P2PBroadcastMessage{
				Type:      libp2pMsg.Type,
				SessionID: libp2pMsg.SessionID,
				Payload:   libp2pMsg.Payload,
			}); err != nil {
				log.Errorf("Error handling pubsub message: %v", err)
			}
		} else {
			log.Warnf("No handler for message type: %s", libp2pMsg.Type)
		}
	}
}

// Initialize connects to bootstrap peers and sets up mDNS if enabled
func (n *Network) Initialize(ctx context.Context) error {
	if n.config.ConnectionWaitSeconds > 0 {
		time.Sleep(time.Duration(n.config.ConnectionWaitSeconds) * time.Second)
	}

	// Connect to bootstrap peers
	if len(n.config.BootstrapPeers) > 0 {
		if err := n.connectToBootstrapPeers(ctx); err != nil {
			return fmt.Errorf("failed to connect to bootstrap peers: %w", err)
		}
	}

	// Set up mDNS discovery if enabled
	if n.config.EnableMDNS {
		if err := n.setupMDNS(); err != nil {
			return fmt.Errorf("failed to set up mDNS: %w", err)
		}
	}

	return nil
}

// ID returns the peer ID of this node
func (n *Network) ID() peer.ID {
	return n.host.ID()
}

// Addrs returns the listen addresses of this node as strings
func (n *Network) Addrs() []string {
	addrs := n.host.Addrs()
	result := make([]string, len(addrs))
	for i, addr := range addrs {
		result[i] = addr.String()
	}
	return result
}

func (n *Network) Start() error {
	ctx := context.Background()
	if err := n.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize network: %w", err)
	}
	return nil
}

// Close closes the P2P network
func (n *Network) Close() error {
	n.cancel()
	return n.host.Close()
}

// GetEventBus returns the event bus
func (n *Network) GetEventBus() *eventbus.Bus {
	return n.eventBus
}

// RegisterHandler registers a handler for a specific message type
func (n *Network) RegisterHandler(msgType types.P2PMessageType, handler func(*types.P2PBroadcastMessage) error) error {
	if _, ok := n.handlers[msgType]; ok {
		return fmt.Errorf("handler already registered for message type: %s", msgType)
	}
	n.handlers[msgType] = handler
	return nil
}

// SendMessage sends a message to a specific peer
func (n *Network) SendMessage(to *peer.ID, msgType types.P2PMessageType, sessionID string, payload []byte) error {
	msg := &Message{
		Type:      msgType,
		ID:        fmt.Sprintf("msg-%s-%d", msgType, time.Now().UnixNano()),
		SessionID: sessionID,
		From:      n.host.ID(),
		To:        to,
		Payload:   payload,
		Timestamp: time.Now().Unix(),
	}

	if *to == n.host.ID() {
		go func() {
			if handler, ok := n.handlers[msgType]; ok {
				if err := handler(&types.P2PBroadcastMessage{
					Type:      msg.Type,
					SessionID: msg.SessionID,
					Payload:   msg.Payload,
				}); err != nil {
					n.logger.Errorf("Error handling local message: %v", err)
				}
			} else {
				n.logger.Warnf("No handler for local message type: %v", msgType)
			}
		}()
		return nil
	}
	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()
	stream, err := n.host.NewStream(ctx, *to, protocol.ID(ProtocolID))
	if err != nil {
		return fmt.Errorf("failed to open stream to peer %s: %w", to, err)
	}
	defer stream.Close()

	encoder := json.NewEncoder(stream)
	if err := encoder.Encode(msg); err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	return nil
}

func (n *Network) SendMessageToTopic(msgBytes []byte) error {
	err := n.topic.Publish(n.ctx, msgBytes)
	if err != nil {
		return fmt.Errorf("failed to send message to topic %s: %w", LibP2PTopic, err)
	}
	n.logger.Infof("✅ %s sent message to topic %s", n.host.ID().String(), LibP2PTopic)
	return nil
}

func (n *Network) BroadcastMessage(to *peer.ID, msgType types.P2PMessageType, sessionID string, payload []byte) error {
	msg := &Message{
		Type:      msgType,
		ID:        fmt.Sprintf("msg-%s-%d", msgType, time.Now().UnixNano()),
		SessionID: sessionID,
		From:      n.host.ID(),
		To:        to,
		Payload:   payload,
		Timestamp: time.Now().Unix(),
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	err = n.topic.Publish(n.ctx, msgBytes)
	if err != nil {
		return fmt.Errorf("failed to publish to topic %s: %w", LibP2PTopic, err)
	}
	n.logger.Infof("✅ %s published message to topic %s", n.host.ID().String(), LibP2PTopic)
	return nil
}

// Connect connects to a specific peer
func (n *Network) Connect(ctx context.Context, peerID peer.ID, addrs []string) error {
	var maddrs []multiaddr.Multiaddr
	for _, addr := range addrs {
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return fmt.Errorf("invalid multiaddr %s: %w", addr, err)
		}
		maddrs = append(maddrs, maddr)
	}

	peerInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: maddrs,
	}
	return n.host.Connect(ctx, peerInfo)
}

// GetPeers returns all connected peers
func (n *Network) GetPeers() []peer.ID {
	return n.host.Network().Peers()
}

// handleStream handles an incoming stream
func (n *Network) handleStream(s network.Stream) {
	defer s.Close()

	var msg Message
	decoder := json.NewDecoder(s)
	if err := decoder.Decode(&msg); err != nil {
		n.logger.Errorf("Error decoding message: %s", err)
		return
	}

	if err := n.checkWhitelisted(msg.From); err != nil {
		n.logger.Errorf("Whitelisted check failed: %v", err)
		return
	}

	handler, exists := n.handlers[msg.Type]
	if exists {
		if err := handler(&types.P2PBroadcastMessage{
			Type:      msg.Type,
			SessionID: msg.SessionID,
			Payload:   msg.Payload,
		}); err != nil {
			n.logger.Errorf("Error handling message: %s", err)
		}
	} else {
		n.logger.Warnf("No handler registered for message type: %s", msg.Type)
	}
}

// verifyPeerProtocol verifies if a peer supports our protocol
func (n *Network) verifyPeerProtocol(ctx context.Context, peerID peer.ID) bool {
	protocols, err := n.host.Peerstore().GetProtocols(peerID)
	if err != nil {
		n.logger.Errorf("Failed to get protocols for peer %s: %v", peerID, err)
		return false
	}

	for _, proto := range protocols {
		if string(proto) == ProtocolID {
			return true
		}
	}

	// Try to connect and see if the peer supports our protocol
	stream, err := n.host.NewStream(ctx, peerID, protocol.ID(ProtocolID))
	if err != nil {
		n.logger.Errorf("Peer %s does not support protocol %s: %v", peerID, ProtocolID, err)
		return false
	}
	stream.Close()

	return true
}

// startHeartbeat starts the heartbeat mechanism to maintain connections
func (n *Network) startHeartbeat() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			peers := n.GetPeers()
			topicPeers := []peer.ID{}
			if n.topic != nil {
				topicPeers = n.topic.ListPeers()
			}
			n.logger.Infof("Heartbeat: Currently connected to %d peers, %d topic peers", len(peers), len(topicPeers))

			// If no peers connected, try to reconnect to bootstrap peers
			if len(peers) == 0 {
				n.logger.Warnf("No peers connected, attempting to reconnect to bootstrap peers...")
				if len(n.config.BootstrapPeers) > 0 {
					if err := n.connectToBootstrapPeers(n.ctx); err != nil {
						n.logger.Errorf("Failed to reconnect to bootstrap peers: %v", err)
					}
				}
			} else {
				n.BroadcastMessage(nil, types.P2PMessageTypeHeartbeat, "", fmt.Appendf(nil, "Heartbeat from %s, my unixnano is %d", n.host.ID().String(), time.Now().UnixNano()))
			}

			// Publish heartbeat event
			n.eventBus.Publish(eventbus.EventNetworkHeartbeat, map[string]any{
				"peerCount": len(peers),
				"peers":     peers,
			})
		}
	}
}

// connectToBootstrapPeers connects to the provided bootstrap peers
func (n *Network) connectToBootstrapPeers(ctx context.Context) error {
	successfulConnections := 0

	for _, peerAddr := range n.config.BootstrapPeers {
		addr, err := multiaddr.NewMultiaddr(peerAddr)
		if err != nil {
			n.logger.Errorf("Failed to parse bootstrap peer address %s: %v", peerAddr, err)
			continue
		}

		peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			n.logger.Errorf("Failed to get peer info from address %s: %v", peerAddr, err)
			continue
		}

		// Skip self connection
		if peerInfo.ID == n.host.ID() {
			continue
		}

		// Store bootstrap peer metadata (mark as bootstrap peer for discovery tag verification)
		n.host.Peerstore().Put(peerInfo.ID, "peer-type", "bootstrap")

		if err := n.host.Connect(ctx, *peerInfo); err != nil {
			n.logger.Errorf("Failed to connect to bootstrap peer %s: %v", peerInfo.ID, err)
			continue
		}

		// Verify the peer supports our protocol
		verifyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if !n.verifyPeerProtocol(verifyCtx, peerInfo.ID) {
			n.logger.Errorf("Bootstrap peer %s does not support our protocol %s, disconnecting", peerInfo.ID, ProtocolID)
			n.host.Network().ClosePeer(peerInfo.ID)
			cancel()
			continue
		}
		cancel()

		n.logger.Infof("Successfully connected to bootstrap peer: %s with protocol support", peerInfo.ID)
		successfulConnections++
	}

	if successfulConnections == 0 {
		return fmt.Errorf("failed to connect to any bootstrap peers")
	}

	return nil
}

// setupMDNS sets up multicast DNS discovery
func (n *Network) setupMDNS() error {
	discovery := mdns.NewMdnsService(n.host, DiscoveryTag, &discoveryNotifee{
		host:     n.host,
		eventBus: n.eventBus,
		network:  n,
	})
	return discovery.Start()
}

// discoveryNotifee gets notified when new peers are discovered via mDNS
type discoveryNotifee struct {
	host     host.Host
	eventBus *eventbus.Bus
	network  *Network
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.host.ID() {
		return
	}

	ctx, cancel := context.WithTimeout(n.network.ctx, 10*time.Second)
	defer cancel()

	if err := n.host.Connect(ctx, pi); err != nil {
		log.Errorf("Failed to connect to discovered peer %s: %v", pi.ID, err)
		return
	}

	// Verify the peer supports our protocol
	if !n.network.verifyPeerProtocol(ctx, pi.ID) {
		log.Errorf("Discovered peer %s does not support our protocol %s, disconnecting", pi.ID, ProtocolID)
		n.host.Network().ClosePeer(pi.ID)
		return
	}

	// Note: Discovery tag verification is not needed here since mDNS already filters by DiscoveryTag

	log.Infof("Successfully connected to discovered peer: %s with protocol support and correct discovery tag", pi.ID)
	n.eventBus.Publish(eventbus.EventNetworkPeerConnected, map[string]any{
		"peerID":       pi.ID,
		"discoveryTag": DiscoveryTag,
		"protocolID":   ProtocolID,
		"verified":     true,
	})
}
