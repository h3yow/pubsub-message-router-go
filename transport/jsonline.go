package transport

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
)

// MaxFrameSize bounds a single JSON-line frame (16 MiB). An over-long frame
// is rejected with CodeFrameTooLarge rather than exhausting memory.
const MaxFrameSize = 16 << 20

// JSONLineTransport reads newline-delimited Frame JSON from any local stream
// (file, pipe, unix-socket connection) and ingests each line. Empty lines
// are skipped without comment; every non-empty line yields a verdict.
type JSONLineTransport struct {
	ing Ingester
	r   io.Reader
}

// NewJSONLine constructs a JSON-line stream transport reading from r.
func NewJSONLine(ing Ingester, r io.Reader) *JSONLineTransport {
	return &JSONLineTransport{ing: ing, r: r}
}

// FrameReport is the per-line disposition returned by Serve.
type FrameReport struct {
	Line   int
	Result IngestResult
}

// Serve reads frames until io.EOF (returns nil) or ctx cancellation. Every
// non-empty line is reported, including rejections; reports are delivered on
// the returned channel in read order and the channel is closed at the end.
func (t *JSONLineTransport) Serve(ctx context.Context) (<-chan FrameReport, error) {
	br := bufio.NewReaderSize(t.r, 64*1024)
	out := make(chan FrameReport, 32)
	go func() {
		defer close(out)
		lineNo := 0
		for {
			raw, err := br.ReadBytes('\n')
			if len(raw) > 0 {
				lineNo++
				line := bytes.TrimRight(raw, "\r\n")
				if len(bytes.TrimSpace(line)) > 0 {
					if len(line) > MaxFrameSize {
						emit(ctx, out, FrameReport{Line: lineNo,
							Result: rejected(CodeFrameTooLarge, "frame exceeds 16MiB")})
					} else {
						emit(ctx, out, FrameReport{Line: lineNo, Result: t.ing.Ingest(ctx, line)})
					}
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				emit(ctx, out, FrameReport{Line: lineNo,
					Result: rejected(CodeMalformedJSON, "stream read error: "+err.Error())})
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return out, nil
}

func emit(ctx context.Context, ch chan<- FrameReport, rep FrameReport) {
	select {
	case ch <- rep:
	case <-ctx.Done():
	}
}
