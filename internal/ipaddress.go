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
	"os"
)

func getOutboundIP() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP, nil
}

// GetOutboundIP returns the preferred outbound ip of this node
// If resolution fails, 'unknown' is returned.
func GetOutboundIP() string {
	if ip, err := getOutboundIP(); err == nil {
		return ip.String()
	}
	return "unknown"
}

// GetHostName returns the hostname reported by the kernel.
// If resolution fails, 'unknown' is returned.
func GetHostName() string {
	if host, err := os.Hostname(); err == nil {
		return host
	}
	return "unknown"
}

// GetHostNameOrIP returns the hostname reported by the kernel falling back to outbound ip if hostname could not be resolved
// If resolution fails, 'unknown' is returned.
func GetHostNameOrIP() string {
	if host, err := os.Hostname(); err == nil {
		return host
	}
	if ip, err := getOutboundIP(); err == nil {
		return ip.String()
	}
	return "unknown"
}
