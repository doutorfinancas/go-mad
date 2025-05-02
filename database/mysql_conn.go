package database

import (
	"context"
	"database/sql"
)

type mySQLConn struct {
	conn              *sql.Conn
	openTx            *sql.Tx
	singleTransaction bool
}

func newMySQLConn(ctx context.Context, db *sql.DB, singleTransaction bool) (*mySQLConn, error) {
	dedicatedConn, err := db.Conn(ctx)

	if err != nil {
		return nil, err
	}

	return &mySQLConn{
		conn:              dedicatedConn,
		singleTransaction: singleTransaction,
	}, nil
}

func (c *mySQLConn) Close() error {
	return c.conn.Close()
}

func (c *mySQLConn) Commit() error {
	if c.openTx == nil {
		return nil
	}

	return c.openTx.Commit()
}

func (c *mySQLConn) getTransaction(ctx context.Context) (*sql.Tx, error) {
	if c.openTx != nil {
		return c.openTx, nil
	}

	var err error
	c.openTx, err = c.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return c.openTx, nil
}

func (c *mySQLConn) useTransactionOrDBQueryRow(ctx context.Context, query string) (*sql.Row, error) {
	if c.singleTransaction {
		tx, err := c.getTransaction(ctx)

		if err != nil {
			return nil, err
		}

		return tx.QueryRowContext(ctx, query), nil
	}

	return c.conn.QueryRowContext(ctx, query), nil
}

func (c *mySQLConn) useTransactionOrDBExec(ctx context.Context, query string) (sql.Result, error) {
	if c.singleTransaction {
		tx, err := c.getTransaction(ctx)

		if err != nil {
			return nil, err
		}

		return tx.ExecContext(ctx, query)
	}

	return c.conn.ExecContext(ctx, query)
}
