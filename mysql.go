// Copyright 2016 The sanpingz Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Wrapper for go-sql-driver/mysql to extend with some exported API

package mysql

import (
	// "database/sql"
	// "database/sql/driver"
	"net"

	"github.com/sanpingz/sql"
	"github.com/sanpingz/sql/driver"
)

const (
	MySQLDriverName = "mysql"
)

type MySQLConn struct {
	*mysqlConn
}

type MySQL struct{}

func (d MySQL) Open(dsn string) (driver.Conn, error) {
	var err error

	// New mysqlConn
	mc := &MySQLConn{
		&mysqlConn{
			maxPacketAllowed: maxPacketSize,
			maxWriteSize:     maxPacketSize - 1,
		},
	}
	mc.cfg, err = ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	mc.parseTime = mc.cfg.ParseTime
	mc.strict = mc.cfg.Strict

	// Connect to Server
	if dial, ok := dials[mc.cfg.Net]; ok {
		mc.netConn, err = dial(mc.cfg.Addr)
	} else {
		nd := net.Dialer{Timeout: mc.cfg.Timeout}
		mc.netConn, err = nd.Dial(mc.cfg.Net, mc.cfg.Addr)
	}
	if err != nil {
		return nil, err
	}

	// Enable TCP Keepalives on TCP connections
	if tc, ok := mc.netConn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			// Don't send COM_QUIT before handshake.
			mc.netConn.Close()
			mc.netConn = nil
			return nil, err
		}
	}

	mc.buf = newBuffer(mc.netConn)

	// Set I/O timeouts
	mc.buf.timeout = mc.cfg.ReadTimeout
	mc.writeTimeout = mc.cfg.WriteTimeout

	// Reading Handshake Initialization Packet
	cipher, err := mc.readInitPacket()
	if err != nil {
		mc.cleanup()
		return nil, err
	}

	// Send Client Authentication Packet
	if err = mc.writeAuthPacket(cipher); err != nil {
		mc.cleanup()
		return nil, err
	}

	// Handle response to auth packet, switch methods if possible
	if err = handleAuthResult(mc.mysqlConn, cipher); err != nil {
		// Authentication failed and MySQL has already closed the connection
		// (https://dev.mysql.com/doc/internals/en/authentication-fails.html).
		// Do not send COM_QUIT, just cleanup and return the error.
		mc.cleanup()
		return nil, err
	}

	// Get max allowed packet size
	maxap, err := mc.getSystemVar("max_allowed_packet")
	if err != nil {
		mc.Close()
		return nil, err
	}
	mc.maxPacketAllowed = stringToInt(maxap) - 1
	if mc.maxPacketAllowed < maxPacketSize {
		mc.maxWriteSize = mc.maxPacketAllowed
	}

	// Handle DSN Params
	err = mc.handleParams()
	if err != nil {
		mc.Close()
		return nil, err
	}

	return mc, nil
}

func (mc *MySQLConn) ReadPacket() ([]byte, error) {
	return mc.readPacket()
}

func (mc *MySQLConn) ReadResultOK() error {
	return mc.readResultOK()
}

func (mc *MySQLConn) ReadUntilEOF() error {
	return mc.readUntilEOF()
}

func (mc *MySQLConn) WritePacket(data []byte) error {
	return mc.writePacket(data)
}

func (mc *MySQLConn) HandleOkPacket(data []byte) error {
	return mc.handleOkPacket(data)
}

func (mc *MySQLConn) HandleErrorPacket(data []byte) error {
	return mc.handleErrorPacket(data)
}

func (mc *MySQLConn) DiscardResults() error {
	return mc.discardResults()
}

func (mc *MySQLConn) ResetSequence() {
	mc.sequence = 0
}

func (mc *MySQLConn) Config() *Config {
	return mc.cfg
}

func init() {
	sql.Register(MySQLDriverName, &MySQL{})
}
