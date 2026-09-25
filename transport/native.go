package transport

import "context"

// NativeTransport is an in-process direct-call transport. Callers hand in
// already-serialized normalized frames; it exists to prove that even the
// simplest local transport passes through the same validation pipeline and
// gets the same distinguishable rejections as any wire transport.
type NativeTransport struct {
	ing Ingester
}

// NewNative constructs a native transport writing into ing.
func NewNative(ing Ingester) *NativeTransport { return &NativeTransport{ing: ing} }

// Send delivers one normalized frame synchronously.
func (t *NativeTransport) Send(ctx context.Context, frame []byte) IngestResult {
	return t.ing.Ingest(ctx, frame)
}

// SendMessage serializes a Frame and sends it.
func (t *NativeTransport) SendMessage(ctx context.Context, f Frame) (IngestResult, error) {
	raw, err := MarshalFrame(f)
	if err != nil {
		return IngestResult{}, err
	}
	return t.Send(ctx, raw), nil
}
