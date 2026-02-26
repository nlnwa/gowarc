/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package internal

import (
	"net"
	"testing"
)

func TestGetOutboundIP(t *testing.T) {
	result := GetOutboundIP()

	// Should return either a valid IP or "unknown"
	if result == "" {
		t.Error("GetOutboundIP() returned empty string")
	}

	if result != "unknown" {
		// If not "unknown", should be a valid IP address
		ip := net.ParseIP(result)
		if ip == nil {
			t.Errorf("GetOutboundIP() returned invalid IP: %q", result)
		}
	}

	t.Logf("GetOutboundIP() = %q", result)
}

func TestGetHostName(t *testing.T) {
	result := GetHostName()

	// Should return either a hostname or "unknown"
	if result == "" {
		t.Error("GetHostName() returned empty string")
	}

	t.Logf("GetHostName() = %q", result)
}

func TestGetHostNameOrIP(t *testing.T) {
	result := GetHostNameOrIP()

	// Should never return empty string
	if result == "" {
		t.Error("GetHostNameOrIP() returned empty string")
	}

	// Should return either a hostname, IP, or "unknown"
	if result != "unknown" {
		// Could be hostname (any string) or IP
		// If it's an IP, verify it's valid
		if ip := net.ParseIP(result); ip != nil {
			t.Logf("GetHostNameOrIP() returned IP: %q", result)
		} else {
			t.Logf("GetHostNameOrIP() returned hostname: %q", result)
		}
	} else {
		t.Logf("GetHostNameOrIP() returned: %q", result)
	}
}

func TestGetHostNameOrIP_FallbackLogic(t *testing.T) {
	// This test verifies that GetHostNameOrIP returns a value
	// The actual value depends on the system, but it should prioritize
	// hostname over IP over "unknown"

	hostname := GetHostName()
	ip := GetOutboundIP()
	hostnameOrIP := GetHostNameOrIP()

	if hostname != "unknown" {
		// If hostname is available, GetHostNameOrIP should return it
		if hostnameOrIP != hostname {
			t.Errorf("Expected GetHostNameOrIP() to return hostname %q, got %q", hostname, hostnameOrIP)
		}
	} else if ip != "unknown" {
		// If hostname is not available but IP is, should return IP
		if hostnameOrIP != ip {
			t.Errorf("Expected GetHostNameOrIP() to return IP %q, got %q", ip, hostnameOrIP)
		}
	} else {
		// Both failed, should return "unknown"
		if hostnameOrIP != "unknown" {
			t.Errorf("Expected GetHostNameOrIP() to return 'unknown', got %q", hostnameOrIP)
		}
	}
}

func Test_getOutboundIP(t *testing.T) {
	ip, err := getOutboundIP()

	if err != nil {
		// Error is acceptable - the system might not have network access
		t.Logf("getOutboundIP() error (might be expected in some environments): %v", err)
		return
	}

	if ip == nil {
		t.Error("getOutboundIP() returned nil IP with no error")
		return
	}

	// Verify it's a valid IP
	if ip.String() == "" {
		t.Error("getOutboundIP() returned IP with empty string representation")
	}

	// IP should be either IPv4 or IPv6
	if ip.To4() == nil && ip.To16() == nil {
		t.Errorf("getOutboundIP() returned invalid IP: %v", ip)
	}

	t.Logf("getOutboundIP() = %v", ip)
}
