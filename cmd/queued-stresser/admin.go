package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
)

// AdminRespOK - successful admin port response.
const AdminRespOK = "Ok"

// Admin - queued admin port client.
type Admin struct {
	conn    net.Conn
	scanner *bufio.Scanner
}

// NewAdmin connects opt.AdminPort.
func NewAdmin(ctx context.Context, opt *Options) (Admin, error) {
	dialer := net.Dialer{Timeout: opt.Timeout}

	conn, err := dialer.DialContext(ctx, "tcp", opt.AdminPort)
	if err != nil {
		return Admin{}, err
	}

	adm := Admin{
		conn:    conn,
		scanner: bufio.NewScanner(conn),
	}

	if !(adm.scanner.Scan() && adm.scanner.Scan()) { //nolint:staticcheck
		return Admin{}, errors.New("error reading queued admin greeting")
	}

	return adm, nil
}

// Close calls the underlying Close call ignoring the result.
func (adm Admin) Close() {
	_ = adm.conn.Close()
}

// DestroyStorage performs the `set destroy_storage` command.
// TODO handle context cancellation
func (adm Admin) DestroyStorage(_ context.Context, queueID int) error {
	_, err := fmt.Fprintf(adm.conn, "set destroy_storage = %d\r\n", queueID)
	if err != nil {
		return err
	}

	if !adm.scanner.Scan() {
		return errors.New("admin port: unexpected disconnect")
	}

	if resp := adm.scanner.Text(); resp != AdminRespOK {
		return errors.New("admin port: unexpected response: " + resp)
	}

	return nil
}
