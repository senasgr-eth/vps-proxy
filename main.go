package main

import (
	"context"
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
	Name  string   `json:"name"`
	Coins []string `json:"coins"`
}

type Config struct {
	Listen           string  `json:"listen"`
	TunnelListen     string  `json:"tunnel_listen"`
	DefaultGroup     string  `json:"default_group"`
	FailbackCooldown string  `json:"failback_cooldown"`
	Groups           []Group `json:"groups"`
}

type ConfigManager struct {
	mu  sync.RWMutex
	cfg *Config
}

func (cm *ConfigManager) Get() *Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.cfg
}

func (cm *ConfigManager) Set(cfg *Config) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cfg = cfg
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

	if cfg.Listen == "" {
		return nil, errors.New("listen address is empty")
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
	}

	return &cfg, nil
}

// Tunnel represents an idle connection from a Tunnel Agent.
type Tunnel struct {
	conn     net.Conn
	group    string
	priority int
	addedAt  time.Time
}

type GroupState struct {
	LastFailoverTime time.Time
}

// TunnelManager coordinates idle tunnel connections thread-safely.
type TunnelManager struct {
	mu         sync.Mutex
	tunnels    map[string][]*Tunnel
	groupState map[string]*GroupState
}

func NewTunnelManager() *TunnelManager {
	return &TunnelManager{
		tunnels:    make(map[string][]*Tunnel),
		groupState: make(map[string]*GroupState),
	}
}

func (tm *TunnelManager) Add(t *Tunnel) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.tunnels[t.group] = append(tm.tunnels[t.group], t)
	log.Printf("[TunnelManager] Registered tunnel for group %s (priority: %d, active for group: %d)",
		t.group, t.priority, len(tm.tunnels[t.group]))
}

// CleanDeadTunnels checks all idle tunnels and removes closed connections.
func (tm *TunnelManager) CleanDeadTunnels() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for grp, list := range tm.tunnels {
		var active []*Tunnel
		for _, t := range list {
			// Zero-byte read check by setting a microscopic timeout
			_ = t.conn.SetReadDeadline(time.Now().Add(1 * time.Microsecond))
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

// PopBest pops the highest priority (lowest integer value) idle tunnel for the given group.
// If multiple tunnels have the same priority, selects the oldest (FIFO).
// If in cooldown, prefers backup tunnels (priority > 1) over primary tunnels (priority 1).
func (tm *TunnelManager) PopBest(group string, cooldownDuration time.Duration) (net.Conn, int, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	list, exists := tm.tunnels[group]
	if !exists || len(list) == 0 {
		return nil, 0, errors.New("no idle tunnels available")
	}

	state, ok := tm.groupState[group]
	if !ok {
		state = &GroupState{}
		tm.groupState[group] = state
	}

	inCooldown := false
	if !state.LastFailoverTime.IsZero() && time.Since(state.LastFailoverTime) < cooldownDuration {
		inCooldown = true
	}

	// Group tunnels by priority
	byPriority := make(map[int][]*Tunnel)
	minPriorityAvailable := 999999
	for _, t := range list {
		byPriority[t.priority] = append(byPriority[t.priority], t)
		if t.priority < minPriorityAvailable {
			minPriorityAvailable = t.priority
		}
	}

	var selectedPriority int

	if inCooldown {
		// Prefer backups (priority > 1) during cooldown.
		// Find lowest backup priority number (e.g. 2 is preferred over 3)
		bestBackupPriority := 999999
		for p := range byPriority {
			if p > 1 && p < bestBackupPriority {
				bestBackupPriority = p
			}
		}

		if bestBackupPriority != 999999 {
			selectedPriority = bestBackupPriority
		} else {
			// No backups online, break cooldown and use primary
			selectedPriority = minPriorityAvailable
		}
	} else {
		// Normal mode: prefer highest priority (lowest integer, e.g. Priority 1)
		selectedPriority = minPriorityAvailable
	}

	// Filter for candidate tunnels matching selectedPriority
	var candidateIdx = -1
	var oldestTime time.Time

	for i, t := range list {
		if t.priority == selectedPriority {
			if candidateIdx == -1 || t.addedAt.Before(oldestTime) {
				candidateIdx = i
				oldestTime = t.addedAt
			}
		}
	}

	if candidateIdx == -1 {
		return nil, 0, errors.New("no matching priority tunnel found")
	}

	bestTunnel := list[candidateIdx]

	// Detect failover transition (Normal mode -> switching to backup)
	if !inCooldown && selectedPriority > 1 {
		state.LastFailoverTime = time.Now()
		log.Printf("[TunnelManager] Failover detected for group %s. Switched to backup (priority %d). Locked on backup for %v.",
			group, selectedPriority, cooldownDuration)
	}

	// Remove from slice
	tm.tunnels[group] = append(list[:candidateIdx], list[candidateIdx+1:]...)

	return bestTunnel.conn, bestTunnel.priority, nil
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

func main() {
	configPath := "backends.json"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("Starting Tunnel Stratum Proxy Server...")

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Fatal error loading config: %v", err)
	}

	cm := &ConfigManager{cfg: cfg}
	tm := NewTunnelManager()

	// Start configuration hot-reloader
	go watchConfig(configPath, cm)

	// Start periodic dead tunnel cleaner every 10 seconds
	go func() {
		for {
			time.Sleep(10 * time.Second)
			tm.CleanDeadTunnels()
		}
	}()

	// Start tunnel acceptance port
	tunnelListener, err := net.Listen("tcp", cfg.TunnelListen)
	if err != nil {
		log.Fatalf("Fatal error starting tunnel listener on %s: %v", cfg.TunnelListen, err)
	}
	defer tunnelListener.Close()
	log.Printf("Listening for Tunnel Agents on TCP %s", cfg.TunnelListen)

	// Start miner acceptance port
	minerListener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		log.Fatalf("Fatal error starting miner listener on %s: %v", cfg.Listen, err)
	}
	defer minerListener.Close()
	log.Printf("Listening for Miners on TCP %s", cfg.Listen)

	// Graceful shutdown setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %s, shutting down...", sig)
		cancel()
		_ = tunnelListener.Close()
		_ = minerListener.Close()
	}()

	// Run acceptance loops
	go runTunnelAcceptLoop(ctx, tunnelListener, tm)
	runMinerAcceptLoop(ctx, minerListener, cm, tm)
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

			oldCfg := cm.Get()
			if oldCfg.Listen != newCfg.Listen || oldCfg.TunnelListen != newCfg.TunnelListen {
				log.Printf("WARNING: Listen/TunnelListen addresses changed in config, but this requires a manual restart to take effect.")
			}

			cm.Set(newCfg)
			lastModTime = fi.ModTime()
			log.Printf("Config successfully hot-reloaded")
		}
	}
}

func runTunnelAcceptLoop(ctx context.Context, listener net.Listener, tm *TunnelManager) {
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

		go handleTunnelRegistration(conn, tm)
	}
}

func handleTunnelRegistration(conn net.Conn, tm *TunnelManager) {
	clientAddr := conn.RemoteAddr().String()

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(3 * time.Minute)
	}

	// Read registration line: "REG <group_name> <priority>\n"
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var line []byte
	buf := make([]byte, 1)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("[%s] Tunnel registration read error: %v", clientAddr, err)
			_ = conn.Close()
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
			_ = conn.Close()
			return
		}
	}
	_ = conn.SetReadDeadline(time.Time{}) // Reset deadline

	parts := strings.Fields(strings.TrimSpace(string(line)))
	if len(parts) != 3 || parts[0] != "REG" {
		log.Printf("[%s] Invalid tunnel registration prefix: %q", clientAddr, string(line))
		_ = conn.Close()
		return
	}

	groupName := parts[1]
	var priority int
	_, err := fmt.Sscanf(parts[2], "%d", &priority)
	if err != nil {
		log.Printf("[%s] Invalid tunnel registration priority: %q", clientAddr, parts[2])
		_ = conn.Close()
		return
	}

	t := &Tunnel{
		conn:     conn,
		group:    groupName,
		priority: priority,
		addedAt:  time.Now(),
	}
	tm.Add(t)
}

func runMinerAcceptLoop(ctx context.Context, listener net.Listener, cm *ConfigManager, tm *TunnelManager) {
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

		go handleMiner(clientConn, cm, tm)
	}
}

func handleMiner(clientConn net.Conn, cm *ConfigManager, tm *TunnelManager) {
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

	// 2. Scan for coin symbol
	payloadStr := string(firstChunk)
	cfg := cm.Get()
	var matchedGroup *Group
	var matchedCoin string

	for i := range cfg.Groups {
		g := &cfg.Groups[i]
		for _, coin := range g.Coins {
			tag := "c=" + coin
			if strings.Contains(strings.ToLower(payloadStr), strings.ToLower(tag)) {
				matchedGroup = g
				matchedCoin = coin
				break
			}
		}
		if matchedGroup != nil {
			break
		}
	}

	// 3. Fallback to default group
	if matchedGroup == nil {
		for i := range cfg.Groups {
			if cfg.Groups[i].Name == cfg.DefaultGroup {
				matchedGroup = &cfg.Groups[i]
				break
			}
		}
	}

	if matchedGroup == nil {
		log.Printf("[%s] No matching group or default group found for payload %q", clientAddr, strings.TrimSpace(payloadStr))
		_ = clientConn.Close()
		return
	}

	// 4. Pop the best tunnel and write the first chunk.
	// We wait up to 3 seconds for a tunnel connection to become available.
	var tunnelConn net.Conn
	var tunnelPriority int
	var popErr error
	startWait := time.Now()

	// Parse cooldown duration
	cooldown := 8 * time.Hour
	if cfg.FailbackCooldown != "" {
		if d, err := time.ParseDuration(cfg.FailbackCooldown); err == nil {
			cooldown = d
		}
	}

	for {
		tunnelConn, tunnelPriority, popErr = tm.PopBest(matchedGroup.Name, cooldown)
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

	if matchedCoin != "" {
		log.Printf("[%s] Routed to priority %d tunnel for group %s (coin: c=%s)", clientAddr, tunnelPriority, matchedGroup.Name, matchedCoin)
	} else {
		log.Printf("[%s] Routed to priority %d tunnel for group %s (default fallback)", clientAddr, tunnelPriority, matchedGroup.Name)
	}

	// 5. Pipe connection bidirectionally
	var bytesSent int64 = int64(len(firstChunk))
	var bytesReceived int64
	startTime := time.Now()

	done := make(chan struct{}, 1)

	// Client -> Tunnel
	go func() {
		cw := CountedWriter{w: tunnelConn, c: &bytesSent}
		_, _ = io.Copy(cw, clientConn)
		_ = tunnelConn.Close()
		_ = clientConn.Close()
		done <- struct{}{}
	}()

	// Tunnel -> Client
	cw := CountedWriter{w: clientConn, c: &bytesReceived}
	_, _ = io.Copy(cw, tunnelConn)
	_ = clientConn.Close()
	_ = tunnelConn.Close()
	<-done

	duration := time.Since(startTime)
	log.Printf("[%s] Connection closed. Group: %s | Priority: %d | Duration: %s | Bytes Sent (Client->Tunnel): %d | Bytes Rcvd (Tunnel->Client): %d",
		clientAddr, matchedGroup.Name, tunnelPriority, duration.Truncate(time.Second), atomic.LoadInt64(&bytesSent), atomic.LoadInt64(&bytesReceived))
}
