package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

//go:embed admin.html
var adminUI embed.FS

type Group struct {
	Name         string   `json:"name"`
	Listen       string   `json:"listen"`
	Coins        []string `json:"coins"`
	MagicBytes   []string `json:"magic_bytes"`
	VersionProto int      `json:"version_proto"`
}

type Config struct {
	Listen         string  `json:"listen"`
	TunnelListen   string  `json:"tunnel_listen"`
	SecretToken    string  `json:"secret_token"`
	TLSCert        string  `json:"tls_cert"`
	TLSKey         string  `json:"tls_key"`
	AdminListen    string  `json:"admin_listen"`
	AdminToken     string  `json:"admin_token"`
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
		if cfg.Listen == "" && g.Listen == "" {
			return nil, fmt.Errorf("group %s does not specify a dedicated listen address, and no global listen address is configured", g.Name)
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
	var globalListener net.Listener

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
		if globalListener != nil {
			_ = globalListener.Close()
		}
		for _, ml := range minerListeners {
			_ = ml.Close()
		}
	})

	// Start global miner listener if configured
	if cfg.Listen != "" {
		l, err := net.Listen("tcp", cfg.Listen)
		if err != nil {
			log.Fatalf("Fatal error starting global miner listener on %s: %v", cfg.Listen, err)
		}
		globalListener = l
		log.Printf("Listening for Global Miners on TCP %s (dynamic routing by coin symbol)", cfg.Listen)

		safeGo("minerAcceptLoop_global", func() {
			runMinerAcceptLoop(ctx, globalListener, cm, tm, "")
		})
	}

	// Start dedicated miner acceptance ports per group if configured
	for i := range cfg.Groups {
		g := &cfg.Groups[i]
		if g.Listen != "" {
			listener, err := net.Listen("tcp", g.Listen)
			if err != nil {
				log.Fatalf("Fatal error starting dedicated miner listener on %s for group %s: %v", g.Listen, g.Name, err)
			}
			minerListeners = append(minerListeners, listener)
			log.Printf("Listening for Dedicated Miners on TCP %s for group %s", g.Listen, g.Name)

			groupName := g.Name
			ml := listener
			safeGo("minerAcceptLoop_"+groupName, func() {
				runMinerAcceptLoop(ctx, ml, cm, tm, groupName)
			})
		}
	}

	// Run tunnel acceptance loop
	safeGo("tunnelAcceptLoop", func() {
		runTunnelAcceptLoop(ctx, tunnelListener, tm, cm)
	})

	// Start admin HTTP server
	if cfg.AdminListen != "" {
		adminMux := http.NewServeMux()
		adminMux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
			handleAdminAPI(w, r, cm, tm)
		})
		adminMux.HandleFunc("/api/groups", func(w http.ResponseWriter, r *http.Request) {
			handleAdminAPI(w, r, cm, tm)
		})
		adminMux.HandleFunc("/api/groups/", func(w http.ResponseWriter, r *http.Request) {
			handleAdminAPI(w, r, cm, tm)
		})
		adminMux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
			handleAdminAPI(w, r, cm, tm)
		})
		adminMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			content, _ := adminUI.ReadFile("admin.html")
			w.Header().Set("Content-Type", "text/html")
			w.Write(content)
		})

		adminServer := &http.Server{
			Addr:    cfg.AdminListen,
			Handler: adminAuthMiddleware(cfg.AdminToken, adminMux),
		}

		safeGo("adminServer", func() {
			log.Printf("Admin UI listening on http://%s", cfg.AdminListen)
			if err := adminServer.ListenAndServe(); err != http.ErrServerClosed {
				log.Printf("Admin server error: %v", err)
			}
		})

		// Graceful shutdown
		safeGo("adminShutdown", func() {
			<-ctx.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer shutdownCancel()
			_ = adminServer.Shutdown(shutdownCtx)
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

func configHasMagicBytes(cfg *Config) bool {
	for _, g := range cfg.Groups {
		if len(g.MagicBytes) > 0 {
			return true
		}
	}
	return false
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

	if groupName == "" {
		routeByMagicBytes(clientAddr, clientConn, cm, tm)
		return
	}

	// Dedicated port: read first chunk and forward to matching group tunnel.
	_ = clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	firstChunk := make([]byte, 1024)
	n, err := clientConn.Read(firstChunk)
	if err != nil {
		log.Printf("[%s] Error reading first packet from miner: %v", clientAddr, err)
		_ = clientConn.Close()
		return
	}
	firstChunk = firstChunk[:n]
	_ = clientConn.SetReadDeadline(time.Time{})

	cfg := cm.Get()
	var matchedGroup *Group
	for i := range cfg.Groups {
		if cfg.Groups[i].Name == groupName {
			matchedGroup = &cfg.Groups[i]
			break
		}
	}
	if matchedGroup == nil {
		log.Printf("[%s] Routing failed: group %s not found in config", clientAddr, groupName)
		_ = clientConn.Close()
		return
	}

	var tunnelConn net.Conn
	var popErr error
	startWait := time.Now()
	isP2P := len(matchedGroup.MagicBytes) > 0
	for {
		tunnelConn, popErr = tm.Pop(matchedGroup.Name)
		if popErr == nil {
			var payload []byte
			if isP2P {
				// P2P group: no PROXY header, send data directly
				payload = firstChunk
			} else {
				// Stratum group: prepend PROXY header so pool sees real miner IP
				proxyHeader := proxyProtoHeader(clientConn)
				payload = append(proxyHeader, firstChunk...)
			}
			_, writeErr := tunnelConn.Write(payload)
			if writeErr == nil {
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

	var bytesSent int64 = int64(len(firstChunk))
	var bytesReceived int64
	startTime := time.Now()
	done := make(chan struct{}, 1)

	safeGo("pipe_client_to_tunnel_"+clientAddr, func() {
		cw := CountedWriter{w: tunnelConn, c: &bytesSent}
		_, _ = io.Copy(cw, clientConn)
		_ = tunnelConn.Close()
		_ = clientConn.Close()
		done <- struct{}{}
	})

	cw := CountedWriter{w: clientConn, c: &bytesReceived}
	_, _ = io.Copy(cw, tunnelConn)
	_ = clientConn.Close()
	_ = tunnelConn.Close()
	<-done

	duration := time.Since(startTime)
	log.Printf("[%s] Connection closed. Group: %s | Duration: %s | Bytes Sent (Client->Tunnel): %d | Bytes Rcvd (Tunnel->Client): %d",
		clientAddr, matchedGroup.Name, duration.Truncate(time.Second), atomic.LoadInt64(&bytesSent), atomic.LoadInt64(&bytesReceived))
}

// proxyProtoHeader builds a HAProxy PROXY protocol v1 header from the miner connection.
// Format: "PROXY TCP4 <client_ip> <server_ip> <client_port> <server_port>\r\n"
func proxyProtoHeader(clientConn net.Conn) []byte {
	clientIP, clientPort, err1 := net.SplitHostPort(clientConn.RemoteAddr().String())
	serverIP, serverPort, err2 := net.SplitHostPort(clientConn.LocalAddr().String())
	if err1 != nil || err2 != nil {
		return nil
	}
	proto := "TCP4"
	if strings.Contains(clientIP, ":") {
		proto = "TCP6"
	}
	return []byte(fmt.Sprintf("PROXY %s %s %s %s %s\r\n", proto, clientIP, serverIP, clientPort, serverPort))
}

// routeByMagicBytes handles blockchain P2P connections on the global port.
// It reads the first 4 bytes (magic bytes / pchMessageStart) and routes to the
// matching backend group. For coins with the same magic bytes, it disambiguates
// by reading the version number from the version message.
func routeByMagicBytes(clientAddr string, clientConn net.Conn, cm *ConfigManager, tm *TunnelManager) {
	_ = clientConn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// Read 4 magic bytes
	magic := make([]byte, 4)
	if _, err := io.ReadFull(clientConn, magic); err != nil {
		log.Printf("[%s] Failed to read magic bytes: %v", clientAddr, err)
		_ = clientConn.Close()
		return
	}
	magicHex := hex.EncodeToString(magic)

	_ = clientConn.SetReadDeadline(time.Time{})

	cfg := cm.Get()

	// Find matching groups by magic bytes
	var candidates []*Group
	for i := range cfg.Groups {
		g := &cfg.Groups[i]
		for _, mb := range g.MagicBytes {
			if strings.EqualFold(mb, magicHex) {
				candidates = append(candidates, g)
				break
			}
		}
	}

	if len(candidates) == 0 {
		// No P2P magic match — fall back to stratum coin scanning.
		// Reconstruct the connection with the magic bytes prepended.
		wrapped := &BufferedConn{
			Conn: clientConn,
			r:    io.MultiReader(bytes.NewReader(magic), clientConn),
		}
		routeByProtocol(clientAddr, wrapped, cm, tm)
		return
	}

	var matchedGroup *Group

	if len(candidates) == 1 {
		matchedGroup = candidates[0]
	} else {
		// Collision: multiple groups share same magic bytes.
		// Disambiguate by reading the version message protocol version.
		matchedGroup = resolveByVersion(clientAddr, clientConn, magic, candidates)
		if matchedGroup == nil {
			log.Printf("[%s] Could not disambiguate %d groups for magic %s — dropping",
				clientAddr, len(candidates), magicHex)
			_ = clientConn.Close()
			return
		}
	}

	log.Printf("[%s] Magic %s → group %s", clientAddr, magicHex, matchedGroup.Name)

	// Pop tunnel and forward (magic bytes prepended to buffered data)
	var tunnelConn net.Conn
	startWait := time.Now()
	for {
		var popErr error
		tunnelConn, popErr = tm.Pop(matchedGroup.Name)
		if popErr == nil {
			break
		}
		if time.Since(startWait) > 3*time.Second {
			log.Printf("[%s] Timeout waiting for tunnel in group %s", clientAddr, matchedGroup.Name)
			_ = clientConn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	if tcpConn, ok := tunnelConn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(3 * time.Minute)
	}

	// Write magic bytes to tunnel (no PROXY header for P2P — daemons dont understand it)
	_, _ = tunnelConn.Write(magic)

	// Bidirectional pipe (magic bytes already consumed from client, need to replay them too)
	var bytesSent int64 = 4
	var bytesReceived int64
	startTime := time.Now()
	done := make(chan struct{}, 1)

	safeGo("pipe_client_to_tunnel_p2p_"+clientAddr, func() {
		cw := CountedWriter{w: tunnelConn, c: &bytesSent}
		_, _ = io.Copy(cw, clientConn)
		_ = tunnelConn.Close()
		_ = clientConn.Close()
		done <- struct{}{}
	})

	cw := CountedWriter{w: clientConn, c: &bytesReceived}
	_, _ = io.Copy(cw, tunnelConn)
	_ = clientConn.Close()
	_ = tunnelConn.Close()
	<-done

	duration := time.Since(startTime)
	log.Printf("[%s] P2P closed. Group: %s | Duration: %s | Bytes Sent: %d | Bytes Rcvd: %d",
		clientAddr, matchedGroup.Name, duration.Truncate(time.Second),
		atomic.LoadInt64(&bytesSent), atomic.LoadInt64(&bytesReceived))
}

// resolveByVersion disambiguates between groups with the same magic bytes
// by reading the version number from the Bitcoin P2P version message.
//
// Bitcoin P2P message header after magic:
//
//	 4 bytes magic (already consumed)
//	12 bytes command ("version\0\0\0\0\0")
//	 4 bytes payload length (uint32 LE)
//	 4 bytes checksum
//	 4 bytes version (int32 LE) — first field of version payload
func resolveByVersion(clientAddr string, clientConn net.Conn, magic []byte, candidates []*Group) *Group {
	// Read the rest of the message header: command (12) + payload_len (4) + checksum (4) = 20 bytes
	headerTail := make([]byte, 20)
	if _, err := io.ReadFull(clientConn, headerTail); err != nil {
		log.Printf("[%s] Failed to read version header: %v", clientAddr, err)
		return nil
	}

	// Verify it's a "version" command
	cmd := strings.TrimRight(string(headerTail[:12]), "\x00")
	if cmd != "version" {
		// First message not version — can't disambiguate, use first candidate
		log.Printf("[%s] First message is %q not version — using %s", clientAddr, cmd, candidates[0].Name)
		return candidates[0]
	}

	// Read payload length (uint32 LE)
	payloadLen := int(headerTail[12]) | int(headerTail[13])<<8 | int(headerTail[14])<<16 | int(headerTail[15])<<24
	if payloadLen < 4 || payloadLen > 1024*1024 {
		log.Printf("[%s] Invalid payload length: %d", clientAddr, payloadLen)
		return nil
	}

	// Read payload (we mainly need the first 4 bytes: version)
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(clientConn, payload); err != nil {
		log.Printf("[%s] Failed to read version payload: %v", clientAddr, err)
		return nil
	}

	// Parse version (int32 LE, first 4 bytes of payload)
	protoVersion := int(payload[0]) | int(payload[1])<<8 | int(payload[2])<<16 | int(payload[3])<<24

	log.Printf("[%s] P2P version=%d detected", clientAddr, protoVersion)

	// Match by version_proto
	for _, g := range candidates {
		if g.VersionProto == protoVersion {
			return g
		}
	}

	// No exact version match — try exact match or use first candidate
	for _, g := range candidates {
		if g.VersionProto == 0 {
			log.Printf("[%s] No version_proto match — using %s (version=%d)", clientAddr, g.Name, protoVersion)
			return g
		}
	}

	// Fallback to first candidate
	log.Printf("[%s] Version %d not matched — falling back to %s", clientAddr, protoVersion, candidates[0].Name)
	return candidates[0]
}

// findCoinInText scans text for "c=COIN" patterns and returns the matching group.
// Matches longest symbol first to avoid substring collisions (e.g. NENG_LOW vs NENG).
func findCoinInText(text string, cfg *Config) (*Group, string) {
	textLower := strings.ToLower(text)

	type entry struct {
		symbol string
		group  *Group
	}
	var entries []entry
	for i := range cfg.Groups {
		g := &cfg.Groups[i]
		for _, coin := range g.Coins {
			entries = append(entries, entry{coin, g})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return len(entries[i].symbol) > len(entries[j].symbol)
	})

	for _, e := range entries {
		tag := "c=" + strings.ToLower(e.symbol)
		if strings.Contains(textLower, tag) {
			return e.group, e.symbol
		}
	}
	return nil, ""
}

// routeByProtocol handles global port connections using stateful stratum inspection.
//
// Flow:
//  1. Read miner lines until mining.authorize is seen.
//  2. On mining.subscribe → reply with a fake subscribe response so sequential
//     miners (those that wait for pool ack before sending authorize) proceed.
//  3. Extract coin from mining.authorize password (e.g. "-p c=neng").
//  4. Route to the matching backend tunnel, replay buffered messages.
//  5. Intercept the backend's real subscribe response, convert it to
//     mining.set_extranonce so the miner syncs its extranonce without confusion.
//  6. Bidirectional pipe for the rest of the session.
func routeByProtocol(clientAddr string, clientConn net.Conn, cm *ConfigManager, tm *TunnelManager) {
	clientBufReader := bufio.NewReader(clientConn)

	var bufferedLines [][]byte
	var subscribeIDRaw json.RawMessage
	var matchedGroup *Group
	var matchedCoin string

	_ = clientConn.SetReadDeadline(time.Now().Add(30 * time.Second))

	authorizeFound := false
	for !authorizeFound {
		line, err := clientBufReader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				var msg struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
					Params json.RawMessage `json:"params"`
				}
				if json.Unmarshal(trimmed, &msg) == nil {
					switch msg.Method {
					case "mining.subscribe":
						subscribeIDRaw = msg.ID
						bufferedLines = append(bufferedLines, line)
						idStr := "null"
						if len(msg.ID) > 0 {
							idStr = string(msg.ID)
						}
						fakeResp := fmt.Sprintf(`{"id":%s,"result":[[["mining.set_difficulty","1"],["mining.notify","1"]],"00000000",4],"error":null}`+"\n", idStr)
						if _, wErr := clientConn.Write([]byte(fakeResp)); wErr != nil {
							log.Printf("[%s] Failed sending fake subscribe response: %v", clientAddr, wErr)
							_ = clientConn.Close()
							return
						}

					case "mining.authorize":
						bufferedLines = append(bufferedLines, line)
						cfg := cm.Get()
						var params []json.RawMessage
						if json.Unmarshal(msg.Params, &params) == nil {
							// Check password first (params[1]: "-p c=coin")
							if len(params) >= 2 {
								var password string
								if json.Unmarshal(params[1], &password) == nil {
									matchedGroup, matchedCoin = findCoinInText(password, cfg)
								}
							}
							// Fallback: check username (params[0])
							if matchedGroup == nil && len(params) >= 1 {
								var username string
								if json.Unmarshal(params[0], &username) == nil {
									matchedGroup, matchedCoin = findCoinInText(username, cfg)
								}
							}
						}
						authorizeFound = true

					default:
						bufferedLines = append(bufferedLines, line)
					}
				} else {
					bufferedLines = append(bufferedLines, line)
				}
			}
		}
		if err != nil {
			if !authorizeFound {
				log.Printf("[%s] Connection closed before mining.authorize: %v", clientAddr, err)
				_ = clientConn.Close()
				return
			}
			break
		}
	}
	_ = clientConn.SetReadDeadline(time.Time{})

	if matchedGroup == nil {
		log.Printf("[%s] Routing failed: no matching coin found in mining.authorize", clientAddr)
		_ = clientConn.Close()
		return
	}

	log.Printf("[%s] Coin identified: %s → group %s", clientAddr, matchedCoin, matchedGroup.Name)

	// Pop tunnel with retry (up to 3 seconds).
	var tunnelConn net.Conn
	startWait := time.Now()
	for {
		var popErr error
		tunnelConn, popErr = tm.Pop(matchedGroup.Name)
		if popErr == nil {
			break
		}
		if time.Since(startWait) > 3*time.Second {
			log.Printf("[%s] Timeout waiting for tunnel in group %s", clientAddr, matchedGroup.Name)
			_ = clientConn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	if tcpConn, ok := tunnelConn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(3 * time.Minute)
	}

	// Replay buffered messages (subscribe + any extras + authorize) to backend,
	// preceded by a PROXY protocol header so the pool sees the real miner IP.
	var bytesSent int64
	if header := proxyProtoHeader(clientConn); len(header) > 0 {
		if _, wErr := tunnelConn.Write(header); wErr != nil {
			log.Printf("[%s] Failed writing PROXY protocol header: %v", clientAddr, wErr)
			_ = tunnelConn.Close()
			_ = clientConn.Close()
			return
		}
		bytesSent += int64(len(header))
	}
	for _, bl := range bufferedLines {
		if _, wErr := tunnelConn.Write(bl); wErr != nil {
			log.Printf("[%s] Failed replaying to tunnel: %v", clientAddr, wErr)
			_ = tunnelConn.Close()
			_ = clientConn.Close()
			return
		}
		bytesSent += int64(len(bl))
	}

	log.Printf("[%s] Routed to group %s (coin: %s)", clientAddr, matchedGroup.Name, matchedCoin)

	// Read backend responses, intercept the subscribe response to extract the
	// real extranonce1 and send mining.set_extranonce to the miner instead.
	tunnelBufReader := bufio.NewReader(tunnelConn)
	if subscribeIDRaw != nil {
		_ = tunnelConn.SetReadDeadline(time.Now().Add(15 * time.Second))
		for {
			respLine, err := tunnelBufReader.ReadBytes('\n')
			if len(respLine) > 0 {
				trimmed := bytes.TrimSpace(respLine)
				var resp struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
					Result json.RawMessage `json:"result"`
				}
				isSubscribeResp := json.Unmarshal(trimmed, &resp) == nil &&
					resp.Method == "" &&
					len(resp.ID) > 0 &&
					string(resp.ID) == string(subscribeIDRaw)

				if isSubscribeResp {
					var result []json.RawMessage
					if json.Unmarshal(resp.Result, &result) == nil && len(result) >= 3 {
						var e1 string
						var e2size int
						_ = json.Unmarshal(result[1], &e1)
						_ = json.Unmarshal(result[2], &e2size)
						if e1 != "" {
							setMsg := fmt.Sprintf(`{"id":null,"method":"mining.set_extranonce","params":["%s",%d]}`+"\n", e1, e2size)
							_, _ = clientConn.Write([]byte(setMsg))
							log.Printf("[%s] Sent mining.set_extranonce: %s/%d", clientAddr, e1, e2size)
						}
					}
					// Subscribe response consumed; switch to bidirectional pipe.
					_ = tunnelConn.SetReadDeadline(time.Time{})
					break
				}
				// Forward non-subscribe responses to miner.
				_, _ = clientConn.Write(respLine)
			}
			if err != nil {
				// Timeout or EOF before finding subscribe response — proceed to pipe anyway.
				log.Printf("[%s] Subscribe response not intercepted: %v", clientAddr, err)
				_ = tunnelConn.SetReadDeadline(time.Time{})
				break
			}
		}
	}

	// Bidirectional pipe for the remainder of the session.
	var bytesReceived int64
	startTime := time.Now()
	done := make(chan struct{}, 1)

	clientWrapped := &BufferedConn{Conn: clientConn, r: clientBufReader}
	tunnelWrapped := &BufferedConn{Conn: tunnelConn, r: tunnelBufReader}

	safeGo("pipe_c2t_"+clientAddr, func() {
		cw := CountedWriter{w: tunnelWrapped, c: &bytesSent}
		_, _ = io.Copy(cw, clientWrapped)
		_ = tunnelWrapped.Close()
		_ = clientWrapped.Close()
		done <- struct{}{}
	})

	cw := CountedWriter{w: clientWrapped, c: &bytesReceived}
	_, _ = io.Copy(cw, tunnelWrapped)
	_ = clientWrapped.Close()
	_ = tunnelWrapped.Close()
	<-done

	duration := time.Since(startTime)
	log.Printf("[%s] Connection closed. Group: %s | Duration: %s | Bytes Sent: %d | Bytes Rcvd: %d",
		clientAddr, matchedGroup.Name, duration.Truncate(time.Second),
		atomic.LoadInt64(&bytesSent), atomic.LoadInt64(&bytesReceived))
}

// --- Admin HTTP API ---

func adminAuthMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		cookie, _ := r.Cookie("admin_token")
		cookieOK := cookie != nil && subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(token)) == 1
		headerOK := strings.HasPrefix(auth, "Bearer ") && subtle.ConstantTimeCompare([]byte(auth[7:]), []byte(token)) == 1

		if strings.HasPrefix(r.URL.Path, "/api/") {
			if !headerOK && !cookieOK {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func handleAdminAPI(w http.ResponseWriter, r *http.Request, cm *ConfigManager, tm *TunnelManager) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/")

	switch {
	case path == "status" && r.Method == "GET":
		handleStatus(w, cm, tm)
	case path == "config" && r.Method == "GET":
		handleGetConfig(w, cm)
	case path == "groups" && r.Method == "GET":
		handleListGroups(w, cm)
	case path == "groups" && r.Method == "POST":
		handleAddGroup(w, r, cm)
	case strings.HasPrefix(path, "groups/") && r.Method == "PUT":
		name := strings.TrimPrefix(path, "groups/")
		handleUpdateGroup(w, r, cm, name)
	case strings.HasPrefix(path, "groups/") && r.Method == "DELETE":
		name := strings.TrimPrefix(path, "groups/")
		handleDeleteGroup(w, cm, name)
	default:
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"not found"}`))
	}
}

type StatusResponse struct {
	Version      string            `json:"version"`
	ActiveConns  int64             `json:"active_connections"`
	MaxConns     int              `json:"max_connections"`
	TunnelCounts map[string]int    `json:"tunnel_counts"`
}

func handleStatus(w http.ResponseWriter, cm *ConfigManager, tm *TunnelManager) {
	cfg := cm.Get()
	tm.mu.Lock()
	counts := make(map[string]int)
	for g, tunnels := range tm.tunnels {
		counts[g] = len(tunnels)
	}
	tm.mu.Unlock()

	json.NewEncoder(w).Encode(StatusResponse{
		Version:      Version,
		ActiveConns:  atomic.LoadInt64(&activeConns),
		MaxConns:     cfg.MaxConnections,
		TunnelCounts: counts,
	})
}

func handleGetConfig(w http.ResponseWriter, cm *ConfigManager) {
	json.NewEncoder(w).Encode(cm.Get())
}

func handleListGroups(w http.ResponseWriter, cm *ConfigManager) {
	json.NewEncoder(w).Encode(cm.Get().Groups)
}

func handleAddGroup(w http.ResponseWriter, r *http.Request, cm *ConfigManager) {
	var g Group
	if err := json.NewDecoder(r.Body).Decode(&g); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid JSON"}`))
		return
	}
	if g.Name == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"name is required"}`))
		return
	}

	cm.mu.Lock()
	cfg := cm.cfg
	for _, existing := range cfg.Groups {
		if existing.Name == g.Name {
			cm.mu.Unlock()
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"error":"group name already exists"}`))
			return
		}
	}
	cfg.Groups = append(cfg.Groups, g)
	cm.mu.Unlock()

	log.Printf("[Admin] Group %s added", g.Name)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(g)
}

func handleUpdateGroup(w http.ResponseWriter, r *http.Request, cm *ConfigManager, name string) {
	var g Group
	if err := json.NewDecoder(r.Body).Decode(&g); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid JSON"}`))
		return
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	for i, existing := range cm.cfg.Groups {
		if existing.Name == name {
			g.Name = name // preserve name
			cm.cfg.Groups[i] = g
			log.Printf("[Admin] Group %s updated", name)
			json.NewEncoder(w).Encode(g)
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(`{"error":"group not found"}`))
}

func handleDeleteGroup(w http.ResponseWriter, cm *ConfigManager, name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for i, g := range cm.cfg.Groups {
		if g.Name == name {
			cm.cfg.Groups = append(cm.cfg.Groups[:i], cm.cfg.Groups[i+1:]...)
			log.Printf("[Admin] Group %s deleted", name)
			w.Write([]byte(`{"ok":true}`))
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(`{"error":"group not found"}`))
}
