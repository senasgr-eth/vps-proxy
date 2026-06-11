package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Group struct {
	Name   string `json:"name"`
	Listen string `json:"listen"`
}

type Config struct {
	TunnelListen   string  `json:"tunnel_listen"`
	SecretToken    string  `json:"secret_token"`
	TLSCert        string  `json:"tls_cert"`
	TLSKey         string  `json:"tls_key"`
	Groups         []Group `json:"groups"`
	MaxConnections int     `json:"max_connections"`
}

type ConfigManager struct {
	mu      sync.RWMutex
	cfg     *Config
	tlsCert *tls.Certificate
}

func (cm *ConfigManager) Get() *Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.cfg
}

func (cm *ConfigManager) GetCert() *tls.Certificate {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.tlsCert
}

func (cm *ConfigManager) Set(cfg *Config, cert *tls.Certificate) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cfg = cfg
	cm.tlsCert = cert
}

func loadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file error: %w", err)
	}
	defer file.Close()

	var cfg Config
	dec := json.NewDecoder(file)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode JSON error: %w", err)
	}

	if cfg.TunnelListen == "" {
		return nil, errors.New("tunnel_listen address is empty")
	}

	if len(cfg.Groups) == 0 {
		return nil, errors.New("no backend groups configured")
	}

	for i := range cfg.Groups {
		g := &cfg.Groups[i]
		if g.Name == "" {
			return nil, fmt.Errorf("group at index %d has empty name", i)
		}
		if g.Listen == "" {
			return nil, fmt.Errorf("group %s does not specify a dedicated listen address", g.Name)
		}
	}

	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 10000
	}

	return &cfg, nil
}

// Tunnel represents an idle connection from a Tunnel Agent.
type Tunnel struct {
	conn    net.Conn
	group   string
	addedAt time.Time
}

// TunnelManager coordinates idle tunnel connections thread-safely.
type TunnelManager struct {
	mu      sync.Mutex
	tunnels map[string][]*Tunnel
}

func NewTunnelManager() *TunnelManager {
	return &TunnelManager{
		tunnels: make(map[string][]*Tunnel),
	}
}

func (tm *TunnelManager) Add(t *Tunnel) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.tunnels[t.group] = append(tm.tunnels[t.group], t)
	log.Printf("[TunnelManager] Registered tunnel for group %s (active for group: %d)",
		t.group, len(tm.tunnels[t.group]))
}

// CleanDeadTunnels checks all idle tunnels and removes closed connections.
func (tm *TunnelManager) CleanDeadTunnels() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for grp, list := range tm.tunnels {
		var active []*Tunnel
		for _, t := range list {
			// Zero-byte read check by setting a microscopic timeout (1ms)
			_ = t.conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
			one := make([]byte, 1)
			_, err := t.conn.Read(one)
			_ = t.conn.SetReadDeadline(time.Time{}) // Reset deadline

			// If connection closed (EOF or connection reset)
			if err == io.EOF || (err != nil && !strings.Contains(err.Error(), "timeout")) {
				_ = t.conn.Close()
				continue
			}
			active = append(active, t)
		}
		if len(list) != len(active) {
			log.Printf("[TunnelManager] Cleaned up %d dead tunnels for group %s (remaining active: %d)",
				len(list)-len(active), grp, len(active))
		}
		tm.tunnels[grp] = active
	}
}

// Pop pops the oldest idle tunnel for the given group (FIFO).
func (tm *TunnelManager) Pop(group string) (net.Conn, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	list, exists := tm.tunnels[group]
	if !exists || len(list) == 0 {
		return nil, errors.New("no idle tunnels available")
	}

	bestTunnel := list[0]
	tm.tunnels[group] = list[1:]

	return bestTunnel.conn, nil
}

// CountedWriter wraps an io.Writer and atomically increments a byte counter on success.
type CountedWriter struct {
	w io.Writer
	c *int64
}

func (cw CountedWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	if n > 0 {
		atomic.AddInt64(cw.c, int64(n))
	}
	return n, err
}

var activeConns int64

type TrackedConn struct {
	net.Conn
	closed int32
}

func (tc *TrackedConn) Close() error {
	if atomic.CompareAndSwapInt32(&tc.closed, 0, 1) {
		atomic.AddInt64(&activeConns, -1)
		return tc.Conn.Close()
	}
	return nil
}

func safeGo(name string, f func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[PANIC RECOVERY] Goroutine %s panicked: %v", name, r)
			}
		}()
		f()
	}()
}

const Version = "1.3.0"


func main() {
	configPath := "backends.json"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("Starting Tunnel Stratum Proxy Server v%s...", Version)

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Fatal error loading config: %v", err)
	}

	var tlsCert *tls.Certificate
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			log.Fatalf("Fatal error loading TLS key pair: %v", err)
		}
		tlsCert = &cert
	}

	cm := &ConfigManager{cfg: cfg, tlsCert: tlsCert}
	tm := NewTunnelManager()

	// Start configuration hot-reloader
	safeGo("watchConfig", func() {
		watchConfig(configPath, cm)
	})

	// Start periodic dead tunnel cleaner every 10 seconds
	safeGo("deadTunnelCleaner", func() {
		for {
			time.Sleep(10 * time.Second)
			tm.CleanDeadTunnels()
		}
	})

	// Start tunnel acceptance port (uses raw TCP listener, dynamic TLS handled per connection)
	tunnelListener, errTunnel := net.Listen("tcp", cfg.TunnelListen)
	if errTunnel != nil {
		log.Fatalf("Fatal error starting tunnel listener on %s: %v", cfg.TunnelListen, errTunnel)
	}
	defer tunnelListener.Close()

	if tlsCert != nil {
		log.Printf("Listening for Tunnel Agents on TCP %s (TLS supported dynamically)", cfg.TunnelListen)
	} else {
		log.Printf("Listening for Tunnel Agents on TCP %s (TLS not supported, no certs)", cfg.TunnelListen)
	}

	var minerListeners []net.Listener

	// Start dedicated miner acceptance ports per group
	for i := range cfg.Groups {
		g := &cfg.Groups[i]
		listener, err := net.Listen("tcp", g.Listen)
		if err != nil {
			log.Fatalf("Fatal error starting dedicated miner listener on %s for group %s: %v", g.Listen, g.Name, err)
		}
		minerListeners = append(minerListeners, listener)
		log.Printf("Listening for Dedicated Miners on TCP %s for group %s", g.Listen, g.Name)
	}

	// Graceful shutdown setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	safeGo("signalHandler", func() {
		sig := <-sigChan
		log.Printf("Received signal %s, shutting down...", sig)
		cancel()
		_ = tunnelListener.Close()
		for _, ml := range minerListeners {
			_ = ml.Close()
		}
	})

	// Run acceptance loops
	safeGo("tunnelAcceptLoop", func() {
		runTunnelAcceptLoop(ctx, tunnelListener, tm, cm)
	})

	for i := range cfg.Groups {
		g := &cfg.Groups[i]
		ml := minerListeners[i]
		groupName := g.Name
		safeGo("minerAcceptLoop_"+groupName, func() {
			runMinerAcceptLoop(ctx, ml, cm, tm, groupName)
		})
	}

	// Block main thread until context is done (graceful shutdown)
	<-ctx.Done()
	log.Printf("Proxy server exited.")
}

func watchConfig(path string, cm *ConfigManager) {
	var lastModTime time.Time
	if fi, err := os.Stat(path); err == nil {
		lastModTime = fi.ModTime()
	}

	for {
		time.Sleep(5 * time.Second)
		fi, err := os.Stat(path)
		if err != nil {
			log.Printf("Config watcher error stating config: %v", err)
			continue
		}

		if fi.ModTime().After(lastModTime) {
			log.Printf("Config file %s modified. Reloading...", path)
			newCfg, err := loadConfig(path)
			if err != nil {
				log.Printf("Failed to load new config: %v (keeping current configuration)", err)
				continue
			}

			var newCert *tls.Certificate
			if newCfg.TLSCert != "" && newCfg.TLSKey != "" {
				cert, err := tls.LoadX509KeyPair(newCfg.TLSCert, newCfg.TLSKey)
				if err != nil {
					log.Printf("Failed to load new TLS key pair: %v (keeping current certificates)", err)
					newCert = cm.GetCert()
				} else {
					newCert = &cert
				}
			}

			oldCfg := cm.Get()
			if oldCfg.TunnelListen != newCfg.TunnelListen {
				log.Printf("WARNING: TunnelListen address changed in config, but this requires a manual restart to take effect.")
			}

			cm.Set(newCfg, newCert)
			lastModTime = fi.ModTime()
			log.Printf("Config successfully hot-reloaded")
		}
	}
}

func runTunnelAcceptLoop(ctx context.Context, listener net.Listener, tm *TunnelManager, cm *ConfigManager) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("Error accepting tunnel connection: %v", err)
				continue
			}
		}

		cfg := cm.Get()
		limit := cfg.MaxConnections
		if limit <= 0 {
			limit = 10000
		}
		if atomic.LoadInt64(&activeConns) >= int64(limit) {
			log.Printf("Max connections reached (%d), dropping tunnel connection from %s", limit, conn.RemoteAddr().String())
			_ = conn.Close()
			continue
		}
		atomic.AddInt64(&activeConns, 1)
		tracked := &TrackedConn{Conn: conn}

		safeGo("tunnelRegistration_"+conn.RemoteAddr().String(), func() {
			handleTunnelRegistration(tracked, tm, cm)
		})
	}
}

// BufferedConn wraps a net.Conn and overrides Read to read from a pre-buffered stream first.
type BufferedConn struct {
	net.Conn
	r io.Reader
}

func (bc *BufferedConn) Read(p []byte) (int, error) {
	return bc.r.Read(p)
}

func handleTunnelRegistration(conn net.Conn, tm *TunnelManager, cm *ConfigManager) {
	clientAddr := conn.RemoteAddr().String()

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(3 * time.Minute)
	}

	wrappedConn := conn
	tlsCert := cm.GetCert()

	if tlsCert != nil {
		// Read first byte to detect if it's a TLS Handshake (0x16)
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		firstByte := make([]byte, 1)
		_, err := io.ReadFull(conn, firstByte)
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			log.Printf("[%s] Error reading first byte for TLS detection: %v", clientAddr, err)
			_ = conn.Close()
			return
		}

		// Reconstruct the connection with the first byte prepended
		wrappedConn = &BufferedConn{
			Conn: conn,
			r:    io.MultiReader(bytes.NewReader(firstByte), conn),
		}

		if firstByte[0] == 0x16 {
			// This is a TLS Handshake ClientHello. Wrap in tls.Server
			tlsConfig := &tls.Config{
				Certificates: []tls.Certificate{*tlsCert},
			}
			tlsConn := tls.Server(wrappedConn, tlsConfig)
			// Force handshake check
			_ = tlsConn.SetReadDeadline(time.Now().Add(5 * time.Second))
			err = tlsConn.Handshake()
			_ = tlsConn.SetReadDeadline(time.Time{})
			if err != nil {
				log.Printf("[%s] TLS handshake failed: %v", clientAddr, err)
				_ = wrappedConn.Close()
				return
			}
			wrappedConn = tlsConn
		}
	}

	// Read registration line: "REG <group_name> <token>\n" or "REG <group_name>\n"
	_ = wrappedConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var line []byte
	buf := make([]byte, 1)
	for {
		n, err := wrappedConn.Read(buf)
		if err != nil {
			log.Printf("[%s] Tunnel registration read error: %v", clientAddr, err)
			_ = wrappedConn.Close()
			return
		}
		if n > 0 {
			if buf[0] == '\n' {
				break
			}
			line = append(line, buf[0])
		}
		if len(line) > 128 {
			log.Printf("[%s] Tunnel registration line too long", clientAddr)
			_ = wrappedConn.Close()
			return
		}
	}
	_ = wrappedConn.SetReadDeadline(time.Time{}) // Reset deadline

	parts := strings.Fields(strings.TrimSpace(string(line)))
	cfg := cm.Get()

	// If secret_token is configured on server, registration must have 3 parts: "REG <group> <token>"
	// Otherwise, it has 2 parts: "REG <group>"
	var expectedParts int
	if cfg.SecretToken != "" {
		expectedParts = 3
	} else {
		expectedParts = 2
	}

	if len(parts) != expectedParts || parts[0] != "REG" {
		log.Printf("[%s] Invalid tunnel registration line: %q", clientAddr, string(line))
		_ = wrappedConn.Close()
		return
	}

	groupName := parts[1]

	if cfg.SecretToken != "" {
		token := parts[2]
		if subtle.ConstantTimeCompare([]byte(token), []byte(cfg.SecretToken)) != 1 {
			log.Printf("[%s] Tunnel registration failed: invalid secret token", clientAddr)
			_ = wrappedConn.Close()
			return
		}
	}

	t := &Tunnel{
		conn:    wrappedConn,
		group:   groupName,
		addedAt: time.Now(),
	}
	tm.Add(t)
}

func runMinerAcceptLoop(ctx context.Context, listener net.Listener, cm *ConfigManager, tm *TunnelManager, groupName string) {
	for {
		clientConn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("Error accepting miner connection: %v", err)
				continue
			}
		}

		cfg := cm.Get()
		limit := cfg.MaxConnections
		if limit <= 0 {
			limit = 10000
		}
		if atomic.LoadInt64(&activeConns) >= int64(limit) {
			log.Printf("Max connections reached (%d), dropping miner connection from %s", limit, clientConn.RemoteAddr().String())
			_ = clientConn.Close()
			continue
		}
		atomic.AddInt64(&activeConns, 1)
		tracked := &TrackedConn{Conn: clientConn}

		safeGo("minerConn_"+clientConn.RemoteAddr().String(), func() {
			handleMiner(tracked, cm, tm, groupName)
		})
	}
}

func handleMiner(clientConn net.Conn, cm *ConfigManager, tm *TunnelManager, groupName string) {
	if groupName == "panic_trigger_for_test" {
		panic("simulated test panic")
	}
	clientAddr := clientConn.RemoteAddr().String()

	if tcpConn, ok := clientConn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(3 * time.Minute)
	}

	// 1. Read first packet (up to 512 bytes) with 5 seconds timeout
	_ = clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	firstChunk := make([]byte, 512)
	n, err := clientConn.Read(firstChunk)
	if err != nil {
		log.Printf("[%s] Error reading first packet from miner: %v", clientAddr, err)
		_ = clientConn.Close()
		return
	}
	firstChunk = firstChunk[:n]
	_ = clientConn.SetReadDeadline(time.Time{})

	// 2. Map directly to groupName.
	cfg := cm.Get()
	var matchedGroup *Group
	for i := range cfg.Groups {
		if cfg.Groups[i].Name == groupName {
			matchedGroup = &cfg.Groups[i]
			break
		}
	}

	if matchedGroup == nil {
		log.Printf("[%s] No matching group %q found in configuration", clientAddr, groupName)
		_ = clientConn.Close()
		return
	}

	// 3. Pop a tunnel and write the first chunk.
	// We wait up to 3 seconds for a tunnel connection to become available.
	var tunnelConn net.Conn
	var popErr error
	startWait := time.Now()

	for {
		tunnelConn, popErr = tm.Pop(matchedGroup.Name)
		if popErr == nil {
			// Test the tunnel connection by writing the first chunk
			_, err = tunnelConn.Write(firstChunk)
			if err == nil {
				// Successfully bound to this tunnel!
				break
			}
			log.Printf("[%s] Popped tunnel was dead/closed, discarding and retrying...", clientAddr)
			_ = tunnelConn.Close()
			continue
		}

		if time.Since(startWait) > 3*time.Second {
			log.Printf("[%s] Routing failed: timeout waiting for tunnel in group %s: %v", clientAddr, matchedGroup.Name, popErr)
			_ = clientConn.Close()
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	if tcpConn, ok := tunnelConn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(3 * time.Minute)
	}

	log.Printf("[%s] Routed to tunnel for group %s", clientAddr, matchedGroup.Name)

	// 4. Pipe connection bidirectionally
	var bytesSent int64 = int64(len(firstChunk))
	var bytesReceived int64
	startTime := time.Now()

	done := make(chan struct{}, 1)

	// Client -> Tunnel
	safeGo("pipe_client_to_tunnel_"+clientAddr, func() {
		cw := CountedWriter{w: tunnelConn, c: &bytesSent}
		_, _ = io.Copy(cw, clientConn)
		_ = tunnelConn.Close()
		_ = clientConn.Close()
		done <- struct{}{}
	})

	// Tunnel -> Client
	cw := CountedWriter{w: clientConn, c: &bytesReceived}
	_, _ = io.Copy(cw, tunnelConn)
	_ = clientConn.Close()
	_ = tunnelConn.Close()
	<-done

	duration := time.Since(startTime)
	log.Printf("[%s] Connection closed. Group: %s | Duration: %s | Bytes Sent (Client->Tunnel): %d | Bytes Rcvd (Tunnel->Client): %d",
		clientAddr, matchedGroup.Name, duration.Truncate(time.Second), atomic.LoadInt64(&bytesSent), atomic.LoadInt64(&bytesReceived))
}
