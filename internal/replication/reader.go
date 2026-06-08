package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const outputPlugin = "pgoutput"

// standbyTimeout controls how often we send heartbeats to Postgres.
// Postgres will drop the connection if it doesn't hear from us within wal_sender_timeout (default 60s).
const standbyTimeout = 10 * time.Second

// Message is a decoded WAL message with its position and commit timestamp.
type Message struct {
	LSN        pglogrepl.LSN
	CommitTime time.Time
	Data       pglogrepl.Message
}

// Reader streams logical replication messages from Postgres.
type Reader struct {
	conn        *pgconn.PgConn
	slotName    string
	publication string
	startLSN    pglogrepl.LSN
}

// New opens a replication connection and ensures the replication slot exists.
// startLSN is the WAL position to resume from (0 = start from current tip).
func New(ctx context.Context, dsn, slotName, publication string, startLSN uint64) (*Reader, error) {
	// Replication connections require the "replication=database" parameter.
	conn, err := pgconn.Connect(ctx, dsn+" replication=database")
	if err != nil {
		return nil, fmt.Errorf("replication connect: %w", err)
	}
	r := &Reader{
		conn:        conn,
		slotName:    slotName,
		publication: publication,
		startLSN:    pglogrepl.LSN(startLSN),
	}
	if err := r.ensureSlot(ctx); err != nil {
		conn.Close(ctx)
		return nil, err
	}
	return r, nil
}

// ensureSlot creates the replication slot if it doesn't already exist.
func (r *Reader) ensureSlot(ctx context.Context) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, r.conn, r.slotName, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		// Error code 42710 = duplicate_object — slot already exists, that's fine.
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			return nil
		}
		return fmt.Errorf("create replication slot: %w", err)
	}
	return nil
}

// Start begins streaming WAL messages and returns a channel of decoded messages.
// The channel is closed when streaming stops (context cancelled or connection lost).
func (r *Reader) Start(ctx context.Context) (<-chan Message, error) {
	pluginArgs := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", r.publication),
		"messages 'true'",
	}
	if err := pglogrepl.StartReplication(ctx, r.conn, r.slotName, r.startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return nil, fmt.Errorf("start replication: %w", err)
	}

	ch := make(chan Message, 100)
	go r.stream(ctx, ch)
	return ch, nil
}

func (r *Reader) stream(ctx context.Context, ch chan<- Message) {
	defer close(ch)

	// clientXLogPos tracks the LSN we've consumed — reported to Postgres via heartbeats.
	clientXLogPos := r.startLSN
	nextHeartbeat := time.Now().Add(standbyTimeout)

	// commitTime holds the timestamp of the current transaction's COMMIT message.
	// Individual change messages (INSERT/UPDATE/DELETE) don't carry a timestamp —
	// we attach the surrounding transaction's commit time instead.
	var commitTime time.Time
	var commitLSN pglogrepl.LSN

	for {
		if time.Now().After(nextHeartbeat) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos}); err != nil {
				return
			}
			nextHeartbeat = time.Now().Add(standbyTimeout)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, nextHeartbeat)
		rawMsg, err := r.conn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // deadline hit — just send the heartbeat and keep going
			}
			return
		}

		if _, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				continue
			}
			// Postgres asked for an immediate reply — force heartbeat on next iteration.
			if pkm.ReplyRequested {
				nextHeartbeat = time.Now()
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				continue
			}
			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

			logicalMsg, err := pglogrepl.ParseV2(xld.WALData, true)
			if err != nil {
				continue
			}

			switch m := logicalMsg.(type) {
			case *pglogrepl.CommitMessage:
				commitTime = m.CommitTime
				commitLSN = xld.WALStart
			default:
				select {
				case ch <- Message{LSN: commitLSN, CommitTime: commitTime, Data: logicalMsg}:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (r *Reader) Close(ctx context.Context) {
	r.conn.Close(ctx)
}
