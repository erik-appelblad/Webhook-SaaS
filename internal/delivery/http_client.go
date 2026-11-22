package delivery

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

func newWebhookHTTPClient() *http.Client {
	return NewWebhookHTTPClient(nil)
}

// ParseAllowedPrivateHosts parses a comma-separated list of host:port entries
// permitted for local development webhook delivery.
func ParseAllowedPrivateHosts(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	hosts := make([]string, 0, len(parts))

	for _, part := range parts {
		entry := strings.TrimSpace(part)
		if entry == "" {
			continue
		}
		normalized, err := normalizeHostPort(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid allowed host %q: %w", entry, err)
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		hosts = append(hosts, normalized)
	}

	return hosts, nil
}

// NewWebhookHTTPClient constructs the outbound webhook HTTP client.
// By default it rejects all non-public IP destinations. Exact host:port entries
// in allowedPrivateHosts additionally permit loopback and RFC1918/ULA private
// addresses for local development while continuing to block link-local,
// multicast, unspecified, and reserved ranges.
func NewWebhookHTTPClient(allowedPrivateHosts []string) *http.Client {
	allowed := make(map[string]struct{}, len(allowedPrivateHosts))
	for _, entry := range allowedPrivateHosts {
		if normalized, err := normalizeHostPort(strings.TrimSpace(entry)); err == nil {
			allowed[normalized] = struct{}{}
		}
	}

	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, fmt.Errorf("parse webhook destination: %w", err)
			}
			if _, err := strconv.Atoi(port); err != nil {
				return nil, fmt.Errorf("invalid webhook port: %w", err)
			}

			normalizedHost := normalizeHost(host)
			_, isAllowlisted := allowed[net.JoinHostPort(normalizedHost, port)]

			ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", normalizedHost)
			if err != nil {
				return nil, fmt.Errorf("resolve webhook destination: %w", err)
			}
			for _, ip := range ips {
				if isAllowlisted {
					if !isAllowedDevAddress(ip) {
						return nil, fmt.Errorf("webhook destination resolves to a disallowed IP address")
					}
					continue
				}
				if !isPublicAddress(ip) {
					return nil, fmt.Errorf("webhook destination resolves to a non-public IP address")
				}
			}

			var lastErr error
			for _, ip := range ips {
				connection, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				if err == nil {
					return connection, nil
				}
				lastErr = err
			}
			if lastErr != nil {
				return nil, fmt.Errorf("connect to webhook destination: %w", lastErr)
			}
			return nil, fmt.Errorf("webhook destination has no IP addresses")
		},
	}

	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func normalizeHost(host string) string {
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func normalizeHostPort(entry string) (string, error) {
	host, port, err := net.SplitHostPort(entry)
	if err != nil {
		return "", err
	}
	host = normalizeHost(host)
	if host == "" {
		return "", fmt.Errorf("host is required")
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return "", fmt.Errorf("port must be between 1 and 65535")
	}
	return net.JoinHostPort(host, strconv.Itoa(portNum)), nil
}

func isAllowedDevAddress(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}
	ip = ip.Unmap()
	return isPublicAddress(ip) || ip.IsLoopback() || ip.IsPrivate()
}

func isPublicAddress(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
