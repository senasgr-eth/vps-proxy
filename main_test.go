package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
func startSimulatedAgent(ctx context.Context, t *testing.T, serverAddr, group string, localPoolAddr string, poolSize int, token string, useTLS bool) {
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

					var conn net.Conn
					var err error
					dialer := &net.Dialer{Timeout: 2 * time.Second}
					if useTLS {
						tlsConf := &tls.Config{
							InsecureSkipVerify: true,
						}
						conn, err = tls.DialWithDialer(dialer, "tcp", serverAddr, tlsConf)
					} else {
						conn, err = dialer.Dial("tcp", serverAddr)
					}

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

					// Send registration (simplified format)
					var regHeader string
					if token != "" {
						regHeader = fmt.Sprintf("REG %s %s\n", group, token)
					} else {
						regHeader = fmt.Sprintf("REG %s\n", group)
					}
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
func TestGlobalPortRouting(t *testing.T) {
	dataChan := make(chan string, 10)

	// Start local backend pools
	_, poolNengAddr, cleanupNeng := startMockPool(t, "pool_neng", dataChan)
	defer cleanupNeng()

	_, poolNxeAddr, cleanupNxe := startMockPool(t, "pool_nxe", dataChan)
	defer cleanupNxe()

	// Configure server with global port and coin lists
	cfg := &Config{
		Listen:       "127.0.0.1:0",
		TunnelListen: "127.0.0.1:0",
		Groups: []Group{
			{
				Name:  "group_neng",
				Coins: []string{"NENG", "LTC"},
			},
			{
				Name:  "group_nxe",
				Coins: []string{"NXE", "BTG"},
			},
		},
	}

	cm := &ConfigManager{cfg: cfg}
	tm := NewTunnelManager()

	tunnelListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for tunnels: %v", err)
	}
	defer tunnelListener.Close()
	tunnelServerAddr := tunnelListener.Addr().String()

	globalMinerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for global miners: %v", err)
	}
	defer globalMinerListener.Close()
	globalMinerAddr := globalMinerListener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start accept loops
	go runTunnelAcceptLoop(ctx, tunnelListener, tm, cm)
	go runMinerAcceptLoop(ctx, globalMinerListener, cm, tm, "") // Empty groupName triggers coin scanning

	// Start Agents
	startSimulatedAgent(ctx, t, tunnelServerAddr, "group_neng", poolNengAddr, 3, "", false)
	startSimulatedAgent(ctx, t, tunnelServerAddr, "group_nxe", poolNxeAddr, 3, "", false)

	time.Sleep(300 * time.Millisecond) // Wait for agents

	t.Run("Routes to NENG group from global port (matched NENG symbol)", func(t *testing.T) {
		conn, err := net.Dial("tcp", globalMinerAddr)
		if err != nil {
			t.Fatalf("Dial global miner port error: %v", err)
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
		if resp != "mock_response_from_pool_neng\n" {
			t.Errorf("Unexpected response: %q", resp)
		}
	})

	t.Run("Routes to NXE group from global port (matched NXE symbol)", func(t *testing.T) {
		conn, err := net.Dial("tcp", globalMinerAddr)
		if err != nil {
			t.Fatalf("Dial global miner port error: %v", err)
		}
		defer conn.Close()

		req := `{"method":"mining.subscribe","params":["miner1","c=NXE"]}`
		_, _ = conn.Write([]byte(req))

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("Read response error: %v", err)
		}

		resp := string(buf[:n])
		if resp != "mock_response_from_pool_nxe\n" {
			t.Errorf("Unexpected response: %q", resp)
		}
	})

	t.Run("Fails to route on unrecognized coin symbol", func(t *testing.T) {
		conn, err := net.Dial("tcp", globalMinerAddr)
		if err != nil {
			t.Fatalf("Dial global miner port error: %v", err)
		}
		defer conn.Close()

		req := `{"method":"mining.subscribe","params":["miner1","c=INVALID"]}`
		_, _ = conn.Write([]byte(req))

		// Connection should be closed by proxy
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		buf := make([]byte, 1024)
		_, err = conn.Read(buf)
		if err == nil {
			t.Errorf("Expected connection to be closed, but read succeeded")
		}
	})
}

func TestMultipleMinerPorts(t *testing.T) {
	dataChan := make(chan string, 10)

	// Start local backend pools
	_, poolNengAddr, cleanupNeng := startMockPool(t, "pool_neng", dataChan)
	defer cleanupNeng()

	_, poolNxeAddr, cleanupNxe := startMockPool(t, "pool_nxe", dataChan)
	defer cleanupNxe()

	// Configure server with dedicated ports for both groups
	cfg := &Config{
		TunnelListen: "127.0.0.1:0",
		Groups: []Group{
			{
				Name:   "group_neng",
				Listen: "127.0.0.1:0",
			},
			{
				Name:   "group_nxe",
				Listen: "127.0.0.1:0",
			},
		},
	}

	cm := &ConfigManager{cfg: cfg}
	tm := NewTunnelManager()

	tunnelListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for tunnels: %v", err)
	}
	defer tunnelListener.Close()
	tunnelServerAddr := tunnelListener.Addr().String()

	nengMinerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for NENG miners: %v", err)
	}
	defer nengMinerListener.Close()
	nengMinerAddr := nengMinerListener.Addr().String()

	nxeMinerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for NXE miners: %v", err)
	}
	defer nxeMinerListener.Close()
	nxeMinerAddr := nxeMinerListener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start accept loops
	go runTunnelAcceptLoop(ctx, tunnelListener, tm, cm)
	go runMinerAcceptLoop(ctx, nengMinerListener, cm, tm, "group_neng")
	go runMinerAcceptLoop(ctx, nxeMinerListener, cm, tm, "group_nxe")

	// Start Agents
	startSimulatedAgent(ctx, t, tunnelServerAddr, "group_neng", poolNengAddr, 3, "", false)
	startSimulatedAgent(ctx, t, tunnelServerAddr, "group_nxe", poolNxeAddr, 3, "", false)

	time.Sleep(300 * time.Millisecond) // Wait for agents

	t.Run("Routes to NENG group from dedicated NENG port", func(t *testing.T) {
		conn, err := net.Dial("tcp", nengMinerAddr)
		if err != nil {
			t.Fatalf("Dial NENG miner port error: %v", err)
		}
		defer conn.Close()

		req := `{"method":"mining.subscribe","params":["miner1"]}`
		_, _ = conn.Write([]byte(req))

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("Read response error: %v", err)
		}

		resp := string(buf[:n])
		if resp != "mock_response_from_pool_neng\n" {
			t.Errorf("Unexpected response: %q", resp)
		}
	})

	t.Run("Routes to NXE group from dedicated NXE port", func(t *testing.T) {
		conn, err := net.Dial("tcp", nxeMinerAddr)
		if err != nil {
			t.Fatalf("Dial NXE miner port error: %v", err)
		}
		defer conn.Close()

		req := `{"method":"mining.subscribe","params":["miner1"]}`
		_, _ = conn.Write([]byte(req))

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("Read response error: %v", err)
		}

		resp := string(buf[:n])
		if resp != "mock_response_from_pool_nxe\n" {
			t.Errorf("Unexpected response: %q", resp)
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

	writeConfig := func(maxConns int) {
		cfg := Config{
			TunnelListen:   "127.0.0.1:0",
			MaxConnections: maxConns,
			Groups: []Group{
				{
					Name:   "group_neng",
					Listen: "127.0.0.1:30100",
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

	writeConfig(100)

	cfg, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("Load config error: %v", err)
	}

	cm := &ConfigManager{cfg: cfg}

	// Watch config
	go watchConfig(configPath, cm)

	if cm.Get().MaxConnections != 100 {
		t.Errorf("Expected MaxConnections 100, got %d", cm.Get().MaxConnections)
	}

	// Sleep to guarantee a different modification timestamp on the file system
	time.Sleep(1 * time.Second)

	// Modify config
	writeConfig(200)

	// Wait for reload (5s loop, wait 6s)
	time.Sleep(6 * time.Second)

	if cm.Get().MaxConnections != 200 {
		t.Errorf("Expected MaxConnections 200 after hot-reload, got %d", cm.Get().MaxConnections)
	}
}

func generateTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		t.Fatalf("Failed to generate serial number: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test Proxy"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("Failed to load X509 key pair: %v", err)
	}

	return cert
}

func TestSecurityAuthentication(t *testing.T) {
	dataChan := make(chan string, 10)

	// Start local mock pool
	_, poolAddr, cleanupPool := startMockPool(t, "neng_secure", dataChan)
	defer cleanupPool()

	// Load TLS Certificate dynamically in-memory
	cert := generateTestCertificate(t)

	cfg := &Config{
		TunnelListen: "127.0.0.1:0",
		SecretToken:  "super_secret_auth_token",
		Groups: []Group{
			{
				Name:   "group_neng",
				Listen: "127.0.0.1:0",
			},
		},
	}

	cm := &ConfigManager{cfg: cfg, tlsCert: &cert}
	tm := NewTunnelManager()

	// 1. Start Tunnel Server with standard TCP listener (dynamic TLS/TCP detection)
	tunnelListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start tunnel listener: %v", err)
	}
	defer tunnelListener.Close()
	tunnelAddr := tunnelListener.Addr().String()

	minerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start miner listener: %v", err)
	}
	defer minerListener.Close()
	minerAddr := minerListener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go runTunnelAcceptLoop(ctx, tunnelListener, tm, cm)
	go runMinerAcceptLoop(ctx, minerListener, cm, tm, "group_neng")

	t.Run("Agent registers successfully with correct token and TLS", func(t *testing.T) {
		agentCtx, cancelAgent := context.WithCancel(ctx)
		defer cancelAgent()

		startSimulatedAgent(agentCtx, t, tunnelAddr, "group_neng", poolAddr, 1, "super_secret_auth_token", true)
		time.Sleep(200 * time.Millisecond) // Wait for connection/registration

		// Dial miner
		conn, err := net.Dial("tcp", minerAddr)
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
		if resp != "mock_response_from_neng_secure\n" {
			t.Errorf("Unexpected response: %q", resp)
		}
	})

	t.Run("Agent registration rejected with wrong token", func(t *testing.T) {
		agentCtx, cancelAgent := context.WithCancel(ctx)
		defer cancelAgent()

		startSimulatedAgent(agentCtx, t, tunnelAddr, "group_neng", poolAddr, 1, "wrong_token", true)
		time.Sleep(200 * time.Millisecond)

		// Verification: no idle tunnels should be registered in the TunnelManager
		conn, popErr := tm.Pop("group_neng")
		if popErr == nil {
			conn.Close()
			t.Errorf("Expected tunnel to be rejected, but it was accepted by server")
		}
	})

	t.Run("Agent registers successfully without TLS (raw TCP)", func(t *testing.T) {
		agentCtx, cancelAgent := context.WithCancel(ctx)
		defer cancelAgent()

		// Attempt raw TCP dial to port with correct token
		startSimulatedAgent(agentCtx, t, tunnelAddr, "group_neng", poolAddr, 1, "super_secret_auth_token", false)
		time.Sleep(200 * time.Millisecond)

		// Dial miner
		conn, err := net.Dial("tcp", minerAddr)
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
		if resp != "mock_response_from_neng_secure\n" {
			t.Errorf("Unexpected response: %q", resp)
		}
	})
}

func TestPanicRecovery(t *testing.T) {
	cfg := &Config{
		TunnelListen: "127.0.0.1:0",
		Groups: []Group{
			{
				Name:   "group_neng",
				Listen: "127.0.0.1:0",
			},
		},
	}
	cm := &ConfigManager{cfg: cfg}
	tm := NewTunnelManager()

	minerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer minerListener.Close()
	minerServerAddr := minerListener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run accept loop with trigger override to trigger simulated panic
	go runMinerAcceptLoop(ctx, minerListener, cm, tm, "panic_trigger_for_test")

	// Dial and trigger panic
	conn, err := net.Dial("tcp", minerServerAddr)
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}
	_ = conn.Close()

	// Wait a moment for panic recovery to log and release resources
	time.Sleep(100 * time.Millisecond)

	// Verify the server accept loop is still alive and accepting new connections
	conn2, err := net.Dial("tcp", minerServerAddr)
	if err != nil {
		t.Fatalf("Server Accept loop died after panic: %v", err)
	}
	_ = conn2.Close()
}

func TestMaxConnections(t *testing.T) {
	cfg := &Config{
		TunnelListen:   "127.0.0.1:0",
		MaxConnections: 2, // set limit to 2
		Groups: []Group{
			{
				Name:   "group_neng",
				Listen: "127.0.0.1:0",
			},
		},
	}
	cm := &ConfigManager{cfg: cfg}
	tm := NewTunnelManager()

	// Reset active connections
	atomic.StoreInt64(&activeConns, 0)

	minerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer minerListener.Close()
	minerServerAddr := minerListener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go runMinerAcceptLoop(ctx, minerListener, cm, tm, "group_neng")

	// 1. Establish 1st connection
	conn1, err := net.Dial("tcp", minerServerAddr)
	if err != nil {
		t.Fatalf("Failed to dial 1st: %v", err)
	}
	defer conn1.Close()

	// 2. Establish 2nd connection
	conn2, err := net.Dial("tcp", minerServerAddr)
	if err != nil {
		t.Fatalf("Failed to dial 2nd: %v", err)
	}
	defer conn2.Close()

	// Give a tiny moment for accept loop goroutines to start and increment counter
	time.Sleep(50 * time.Millisecond)

	// 3. Establish 3rd connection - should be closed immediately
	conn3, err := net.Dial("tcp", minerServerAddr)
	if err != nil {
		t.Fatalf("Failed to dial 3rd: %v", err)
	}
	defer conn3.Close()

	// Read from 3rd connection - should get EOF immediately because it was closed
	_ = conn3.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 10)
	_, err = conn3.Read(buf)
	if err == nil {
		t.Errorf("Expected 3rd connection to be closed due to limit, but read succeeded")
	}

	// 4. Close 1st connection, slot becomes available
	_ = conn1.Close()
	time.Sleep(50 * time.Millisecond) // wait for defer close hook

	// 5. Establish 4th connection - should succeed
	conn4, err := net.Dial("tcp", minerServerAddr)
	if err != nil {
		t.Fatalf("Failed to dial 4th after slot freed: %v", err)
	}
	defer conn4.Close()

	// Verify we can read/write on 4th (not dropped immediately)
	_ = conn4.SetWriteDeadline(time.Now().Add(1 * time.Second))
	_, err = conn4.Write([]byte("test"))
	if err != nil {
		t.Errorf("Expected 4th connection to remain open, but write failed: %v", err)
	}
}
