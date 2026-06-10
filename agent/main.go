package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type Mapping struct {
	Group    string `json:"group"`
	Priority int    `json:"priority"`
	Local    string `json:"local"`
}

type Config struct {
	Server        string    `json:"server"`
	PoolSize      int       `json:"pool_size"`
	SecretToken   string    `json:"secret_token"`
	TLSSkipVerify bool      `json:"tls_skip_verify"`
	TLS           bool      `json:"tls"`
	Insecure      bool      `json:"insecure"`
	Mappings      []Mapping `json:"mappings"`
}

func main() {
	configPath := "agent.json"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("Starting Tunnel Stratum Proxy Agent...")

	// Load configuration
	cfgFile, err := os.Open(configPath)
	if err != nil {
		log.Fatalf("Fatal error opening agent config: %v", err)
	}
	defer cfgFile.Close()

	var cfg Config
	dec := json.NewDecoder(cfgFile)
	if err := dec.Decode(&cfg); err != nil {
		log.Fatalf("Fatal error decoding agent config: %v", err)
	}

	if cfg.Server == "" {
		log.Fatalf("Fatal error: Server address is empty")
	}

	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 5
	}

	var wg sync.WaitGroup
	for _, m := range cfg.Mappings {
		wg.Add(1)
		go func(mapping Mapping) {
			defer wg.Done()
			maintainTunnelPool(cfg, mapping)
		}(m)
	}

	wg.Wait()
}

func maintainTunnelPool(cfg Config, mapping Mapping) {
	log.Printf("[PoolManager] Starting pool for group %s (priority: %d, local pool: %s)",
		mapping.Group, mapping.Priority, mapping.Local)

	slots := make(chan struct{}, cfg.PoolSize)
	for i := 0; i < cfg.PoolSize; i++ {
		slots <- struct{}{}
	}

	for {
		<-slots // Wait for an available slot
		
		go func() {
			defer func() {
				slots <- struct{}{} // Release slot on exit
			}()

			runTunnelSession(cfg, mapping)
		}()

		// Small delay to prevent tight-loop CPU spike if VPS drops connections immediately
		time.Sleep(100 * time.Millisecond)
	}
}

func runTunnelSession(cfg Config, mapping Mapping) {
	// 1. Dial VPS (secure TLS by default, raw TCP if configured insecure)
	var vpsConn net.Conn
	var err error
	dialer := &net.Dialer{Timeout: 10 * time.Second}

	if cfg.TLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify,
		}
		vpsConn, err = tls.DialWithDialer(dialer, "tcp", cfg.Server, tlsConfig)
	} else {
		vpsConn, err = dialer.Dial("tcp", cfg.Server)
	}

	if err != nil {
		log.Printf("[Tunnel-%s] Error dialling VPS %s: %v. Retrying in 5s...", mapping.Group, cfg.Server, err)
		time.Sleep(5 * time.Second)
		return
	}
	defer vpsConn.Close()

	if tcpConn, ok := vpsConn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(3 * time.Minute)
	}

	// 2. Send registration header: "REG <group> <priority> <token>\n" or "REG <group> <priority>\n"
	var regHeader string
	if cfg.SecretToken != "" {
		regHeader = fmt.Sprintf("REG %s %d %s\n", mapping.Group, mapping.Priority, cfg.SecretToken)
	} else {
		regHeader = fmt.Sprintf("REG %s %d\n", mapping.Group, mapping.Priority)
	}

	_ = vpsConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = vpsConn.Write([]byte(regHeader))
	if err != nil {
		log.Printf("[Tunnel-%s] Error writing registration: %v", mapping.Group, err)
		return
	}
	_ = vpsConn.SetWriteDeadline(time.Time{}) // Reset write deadline

	// 3. Block reading from VPS.
	// This connection will remain idle until the VPS pops it and sends a miner's first chunk of data.
	firstChunk := make([]byte, 1024)
	n, err := vpsConn.Read(firstChunk)
	if err != nil {
		// Read fails (timeout, close, EOF). Exit to let slot replenish connection.
		return
	}

	// 4. We got data! This means this connection is now ACTIVE.
	// We immediately dial the local backend.
	localConn, err := net.DialTimeout("tcp", mapping.Local, 5*time.Second)
	if err != nil {
		log.Printf("[Tunnel-%s] Failed to dial local pool %s: %v. Closing tunnel.", mapping.Group, mapping.Local, err)
		return
	}
	defer localConn.Close()

	if tcpConn, ok := localConn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(3 * time.Minute)
	}

	// 5. Send first chunk to local pool
	_ = localConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = localConn.Write(firstChunk[:n])
	if err != nil {
		log.Printf("[Tunnel-%s] Failed to forward first packet to local pool: %v", mapping.Group, err)
		return
	}
	_ = localConn.SetWriteDeadline(time.Time{})

	// 6. Pipe connection bidirectionally
	done := make(chan struct{}, 1)

	// VPS -> Local Pool
	go func() {
		_, _ = io.Copy(localConn, vpsConn)
		_ = localConn.Close()
		_ = vpsConn.Close()
		done <- struct{}{}
	}()

	// Local Pool -> VPS
	_, _ = io.Copy(vpsConn, localConn)
	_ = vpsConn.Close()
	_ = localConn.Close()
	<-done
}
