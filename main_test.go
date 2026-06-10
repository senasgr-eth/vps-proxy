package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// startMockPool starts a TCP server simulating a local backend pool.
// It writes "mock_response_from_<id>" when it receives a chunk.
func startMockPool(t *testing.T, id string, dataChan chan<- string) (net.Listener, string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start mock pool %s: %v", id, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}

			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer c.Close()
				buf := make([]byte, 1024)
				n, err := c.Read(buf)
				if err != nil && err != io.EOF {
					return
				}
				if n > 0 {
					dataChan <- id + ":" + string(buf[:n])
					_, _ = c.Write([]byte("mock_response_from_" + id + "\n"))
				}
			}(conn)
		}
	}()

	cleanup := func() {
		cancel()
		_ = l.Close()
		wg.Wait()
	}

	return l, l.Addr().String(), cleanup
}

// startSimulatedAgent runs a background agent loop for testing.
func startSimulatedAgent(ctx context.Context, t *testing.T, serverAddr, group string, priority int, localPoolAddr string, poolSize int) {
	slots := make(chan struct{}, poolSize)
	for i := 0; i < poolSize; i++ {
		slots <- struct{}{}
	}

	var mu sync.Mutex
	var activeConns []net.Conn

	// Close all connections when context is done
	go func() {
		<-ctx.Done()
		mu.Lock()
		for _, c := range activeConns {
			_ = c.Close()
		}
		mu.Unlock()
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-slots:
				go func() {
					defer func() {
						slots <- struct{}{}
					}()

					conn, err := net.DialTimeout("tcp", serverAddr, 2*time.Second)
					if err != nil {
						time.Sleep(50 * time.Millisecond)
						return
					}
					
					mu.Lock()
					select {
					case <-ctx.Done():
						_ = conn.Close()
						mu.Unlock()
						return
					default:
						activeConns = append(activeConns, conn)
					}
					mu.Unlock()

					defer func() {
						_ = conn.Close()
						mu.Lock()
						for idx, c := range activeConns {
							if c == conn {
								activeConns = append(activeConns[:idx], activeConns[idx+1:]...)
								break
							}
						}
						mu.Unlock()
					}()

					// Send registration
					regHeader := fmt.Sprintf("REG %s %d\n", group, priority)
					_, err = conn.Write([]byte(regHeader))
					if err != nil {
						return
					}

					// Read first chunk (miner data)
					firstChunk := make([]byte, 1024)
					n, err := conn.Read(firstChunk)
					if err != nil {
						return
					}

					// Dial local pool
					local, err := net.DialTimeout("tcp", localPoolAddr, 2*time.Second)
					if err != nil {
						return
					}
					defer local.Close()

					_, err = local.Write(firstChunk[:n])
					if err != nil {
						return
					}

					// Pipe
					done := make(chan struct{}, 1)
					go func() {
						_, _ = io.Copy(local, conn)
						done <- struct{}{}
					}()
					_, _ = io.Copy(conn, local)
					<-done
				}()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

func TestRoutingAndFailover(t *testing.T) {
	dataChan := make(chan string, 10)

	// Start local backend pools
	_, poolNeng1Addr, cleanupNeng1 := startMockPool(t, "neng_primary", dataChan)
	defer cleanupNeng1()

	_, poolNeng2Addr, cleanupNeng2 := startMockPool(t, "neng_backup", dataChan)
	defer cleanupNeng2()

	_, poolNxeAddr, cleanupNxe := startMockPool(t, "nxe_pool", dataChan)
	defer cleanupNxe()

	// Configure server
	cfg := &Config{
		Listen:           "127.0.0.1:0",
		TunnelListen:     "127.0.0.1:0",
		DefaultGroup:     "group_neng",
		FailbackCooldown: "1s",
		Groups: []Group{
			{
				Name:  "group_neng",
				Coins: []string{"NENG", "NXE"},
			},
			{
				Name:  "group_nxe",
				Coins: []string{"BTG", "BTB"},
			},
		},
	}

	cm := &ConfigManager{cfg: cfg}
	tm := NewTunnelManager()

	// Start Tunnel Server listeners on OS-allocated ports
	tunnelListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for tunnels: %v", err)
	}
	defer tunnelListener.Close()
	tunnelServerAddr := tunnelListener.Addr().String()

	minerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for miners: %v", err)
	}
	defer minerListener.Close()
	minerServerAddr := minerListener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run server loops
	go runTunnelAcceptLoop(ctx, tunnelListener, tm)
	go runMinerAcceptLoop(ctx, minerListener, cm, tm)

	// Start Agents
	agentAContext, cancelAgentA := context.WithCancel(ctx)
	// Agent A: group_neng, priority 1 (primary)
	startSimulatedAgent(agentAContext, t, tunnelServerAddr, "group_neng", 1, poolNeng1Addr, 3)

	// Agent B: group_neng, priority 2 (backup)
	startSimulatedAgent(ctx, t, tunnelServerAddr, "group_neng", 2, poolNeng2Addr, 3)

	// Agent C: group_nxe, priority 1
	startSimulatedAgent(ctx, t, tunnelServerAddr, "group_nxe", 1, poolNxeAddr, 3)

	// Wait for agents to register and fill connection pools
	time.Sleep(500 * time.Millisecond)

	t.Run("Route to Agent A (Priority 1) for NENG", func(t *testing.T) {
		conn, err := net.Dial("tcp", minerServerAddr)
		if err != nil {
			t.Fatalf("Dial miner port error: %v", err)
		}
		defer conn.Close()

		req := `{"method":"mining.subscribe","params":["miner1","c=NENG"]}`
		_, _ = conn.Write([]byte(req))

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("Read response error: %v", err)
		}

		resp := string(buf[:n])
		if resp != "mock_response_from_neng_primary\n" {
			t.Errorf("Unexpected response: %q", resp)
		}

		select {
		case data := <-dataChan:
			if data != "neng_primary:"+req {
				t.Errorf("Backend got unexpected data: %q", data)
			}
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout waiting for backend data")
		}
	})

	t.Run("Route to Agent C for BTG", func(t *testing.T) {
		conn, err := net.Dial("tcp", minerServerAddr)
		if err != nil {
			t.Fatalf("Dial miner port error: %v", err)
		}
		defer conn.Close()

		req := `{"method":"mining.subscribe","params":["miner1","c=BTG"]}`
		_, _ = conn.Write([]byte(req))

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("Read response error: %v", err)
		}

		resp := string(buf[:n])
		if resp != "mock_response_from_nxe_pool\n" {
			t.Errorf("Unexpected response: %q", resp)
		}

		select {
		case data := <-dataChan:
			if data != "nxe_pool:"+req {
				t.Errorf("Backend got unexpected data: %q", data)
			}
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout waiting for backend data")
		}
	})

	t.Run("Route to Default Group (Agent A, Priority 1) on mismatch", func(t *testing.T) {
		conn, err := net.Dial("tcp", minerServerAddr)
		if err != nil {
			t.Fatalf("Dial miner port error: %v", err)
		}
		defer conn.Close()

		req := `{"method":"mining.subscribe","params":["miner1","c=OTHER"]}`
		_, _ = conn.Write([]byte(req))

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("Read response error: %v", err)
		}

		resp := string(buf[:n])
		if resp != "mock_response_from_neng_primary\n" {
			t.Errorf("Unexpected response: %q", resp)
		}

		select {
		case data := <-dataChan:
			if data != "neng_primary:"+req {
				t.Errorf("Backend got unexpected data: %q", data)
			}
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout waiting for backend data")
		}
	})

	t.Run("Failover to Agent B (Priority 2) when Agent A goes offline", func(t *testing.T) {
		// Stop Agent A
		cancelAgentA()

		// Wait for socket closure propagation
		time.Sleep(200 * time.Millisecond)

		// Clean up its connections in the server manager
		tm.CleanDeadTunnels()

		conn, err := net.Dial("tcp", minerServerAddr)
		if err != nil {
			t.Fatalf("Dial miner port error: %v", err)
		}
		defer conn.Close()

		req := `{"method":"mining.subscribe","params":["miner1","c=NENG"]}`
		_, _ = conn.Write([]byte(req))

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("Read response error: %v", err)
		}

		resp := string(buf[:n])
		if resp != "mock_response_from_neng_backup\n" {
			t.Errorf("Unexpected response: %q", resp)
		}

		select {
		case data := <-dataChan:
			if data != "neng_backup:"+req {
				t.Errorf("Backend got unexpected data: %q", data)
			}
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout waiting for backend data")
		}
	})

	t.Run("Recover and check failback cooldown holds on backup first, then falls back to primary", func(t *testing.T) {
		// 1. Restart Agent A (Priority 1)
		agentA2Context, cancelAgentA2 := context.WithCancel(ctx)
		defer cancelAgentA2()
		startSimulatedAgent(agentA2Context, t, tunnelServerAddr, "group_neng", 1, poolNeng1Addr, 3)

		// Wait for registration
		time.Sleep(300 * time.Millisecond)

		// 2. Dial miner. Since we are within the 1s cooldown (from the previous failover),
		// this connection MUST still route to Agent B (Priority 2) even though Agent A is online.
		conn1, err := net.Dial("tcp", minerServerAddr)
		if err != nil {
			t.Fatalf("Dial miner port error: %v", err)
		}
		req1 := `{"method":"mining.subscribe","params":["miner1","c=NENG"]}`
		_, _ = conn1.Write([]byte(req1))

		buf1 := make([]byte, 1024)
		n1, err := conn1.Read(buf1)
		if err != nil {
			t.Fatalf("Read response error: %v", err)
		}
		resp1 := string(buf1[:n1])
		if resp1 != "mock_response_from_neng_backup\n" {
			t.Errorf("Expected connection to hold on backup due to active cooldown, but got: %q", resp1)
		}
		conn1.Close()

		// Read data from queue
		select {
		case data := <-dataChan:
			if data != "neng_backup:"+req1 {
				t.Errorf("Backend got unexpected data during cooldown: %q", data)
			}
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout waiting for backend data")
		}

		// 3. Wait for the cooldown to expire (total > 1 second since failover)
		time.Sleep(1 * time.Second)

		// 4. Dial miner again. Cooldown has expired, so it must route back to Agent A (Priority 1).
		conn2, err := net.Dial("tcp", minerServerAddr)
		if err != nil {
			t.Fatalf("Dial miner port error: %v", err)
		}
		defer conn2.Close()

		req2 := `{"method":"mining.subscribe","params":["miner1","c=NENG"]}`
		_, _ = conn2.Write([]byte(req2))

		buf2 := make([]byte, 1024)
		n2, err := conn2.Read(buf2)
		if err != nil {
			t.Fatalf("Read response error: %v", err)
		}
		resp2 := string(buf2[:n2])
		if resp2 != "mock_response_from_neng_primary\n" {
			t.Errorf("Expected connection to recover back to primary after cooldown, but got: %q", resp2)
		}

		select {
		case data := <-dataChan:
			if data != "neng_primary:"+req2 {
				t.Errorf("Backend got unexpected data after cooldown: %q", data)
			}
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout waiting for backend data")
		}
	})
}

func TestHotReload(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "proxy-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "backends.json")

	writeConfig := func(defGroup string) {
		cfg := Config{
			Listen:       "127.0.0.1:0",
			TunnelListen: "127.0.0.1:0",
			DefaultGroup: defGroup,
			Groups: []Group{
				{
					Name:  "group_neng",
					Coins: []string{"NENG"},
				},
				{
					Name:  "group_nxe",
					Coins: []string{"NXE"},
				},
			},
		}
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("Marshal config error: %v", err)
		}
		if err := os.WriteFile(configPath, data, 0644); err != nil {
			t.Fatalf("Write config error: %v", err)
		}
	}

	writeConfig("group_neng")

	cfg, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("Load config error: %v", err)
	}

	cm := &ConfigManager{cfg: cfg}

	// Watch config
	go watchConfig(configPath, cm)

	if cm.Get().DefaultGroup != "group_neng" {
		t.Errorf("Expected default group group_neng, got %s", cm.Get().DefaultGroup)
	}

	// Sleep to guarantee a different modification timestamp on the file system
	time.Sleep(1 * time.Second)

	// Modify config
	writeConfig("group_nxe")

	// Wait for reload (5s loop, wait 6s)
	time.Sleep(6 * time.Second)

	if cm.Get().DefaultGroup != "group_nxe" {
		t.Errorf("Expected default group group_nxe after hot-reload, got %s", cm.Get().DefaultGroup)
	}
}
