package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/eventstore"
	"github.com/MahyarNemati/ledger/internal/idempotency"
	"github.com/MahyarNemati/ledger/internal/ledger"
	"github.com/MahyarNemati/ledger/internal/projection"
)

type harness struct {
	t      *testing.T
	store  *eventstore.Memory
	svc    *ledger.Service
	proj   *projection.Memory
	idem   *idempotency.Memory
	server *httptest.Server
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, store: eventstore.NewMemory(), proj: projection.NewMemory(), idem: idempotency.NewMemory()}
	h.svc = ledger.NewService(h.store, ledger.Options{SnapshotEvery: 10})
	h.server = httptest.NewServer(New(Deps{Service: h.svc, Projections: h.proj, Idempotency: h.idem}))
	t.Cleanup(h.server.Close)
	return h
}

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

func (r response) errorCode(t *testing.T) string {
	t.Helper()
	var e ErrorBody
	r.decode(t, &e)
	return e.Error.Code
}

func (h *harness) do(method, path, key string, body any) response {
	h.t.Helper()
	var rdr *bytes.Reader
	switch b := body.(type) {
	case nil:
		rdr = bytes.NewReader(nil)
	case string:
		rdr = bytes.NewReader([]byte(b))
	default:
		buf, _ := json.Marshal(b)
		rdr = bytes.NewReader(buf)
	}
	req, _ := http.NewRequest(method, h.server.URL+path, rdr)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(res.Body)
	return response{status: res.StatusCode, header: res.Header, body: buf.Bytes()}
}

func (h *harness) openAccount(name, typ, currency string) accountResponse {
	h.t.Helper()
	res := h.do("POST", "/v1/accounts", uuid.NewString(), map[string]any{"name": name, "type": typ, "currency": currency})
	if res.status != http.StatusCreated {
		h.t.Fatalf("open account: %d %s", res.status, res.body)
	}
	var a accountResponse
	res.decode(h.t, &a)
	return a
}

func (h *harness) transfer(key string, debit, credit uuid.UUID, amount int64) response {
	h.t.Helper()
	return h.do("POST", "/v1/transfers", key, map[string]any{
		"debit_account_id": debit, "credit_account_id": credit, "amount": amount, "currency": "USD",
	})
}

func (h *harness) account(id uuid.UUID) accountResponse {
	h.t.Helper()
	res := h.do("GET", "/v1/accounts/"+id.String(), "", nil)
	if res.status != http.StatusOK {
		h.t.Fatalf("get account: %d %s", res.status, res.body)
	}
	var a accountResponse
	res.decode(h.t, &a)
	return a
}

func TestTransferIsIdempotent(t *testing.T) {
	h := newHarness(t)
	bank := h.openAccount("Settlement", "asset", "USD")
	alice := h.openAccount("Alice", "liability", "USD")

	first := h.transfer("deposit-1", bank.ID, alice.ID, 10_000)
	if first.status != http.StatusCreated {
		t.Fatalf("transfer: %d %s", first.status, first.body)
	}
	again := h.transfer("deposit-1", bank.ID, alice.ID, 10_000)
	if again.status != http.StatusCreated || !bytes.Equal(first.body, again.body) {
		t.Fatalf("replay differs: %d %s vs %s", again.status, again.body, first.body)
	}
	if again.header.Get("Idempotent-Replayed") != "true" {
		t.Fatal("replay header missing")
	}
	if got := h.account(alice.ID).Balance; got != 10_000 {
		t.Fatalf("balance %d after a replayed transfer, want 10000", got)
	}

	reused := h.transfer("deposit-1", bank.ID, alice.ID, 99)
	if reused.status != http.StatusUnprocessableEntity || reused.errorCode(t) != codeKeyReused {
		t.Fatalf("key reuse: %d %s", reused.status, reused.body)
	}
	if missing := h.transfer("", bank.ID, alice.ID, 1); missing.status != http.StatusBadRequest || missing.errorCode(t) != codeKeyRequired {
		t.Fatalf("missing key: %d %s", missing.status, missing.body)
	}
}

func TestLostIdempotencyRecordDoesNotDoublePost(t *testing.T) {
	h := newHarness(t)
	bank := h.openAccount("Settlement", "asset", "USD")
	alice := h.openAccount("Alice", "liability", "USD")
	first := h.transfer("k-1", bank.ID, alice.ID, 500)

	// Simulate a crash between the ledger commit and storing the response.
	fresh := httptest.NewServer(New(Deps{Service: h.svc, Projections: h.proj, Idempotency: idempotency.NewMemory()}))
	defer fresh.Close()
	h.server = fresh
	second := h.transfer("k-1", bank.ID, alice.ID, 500)

	var a, b entryResponse
	first.decode(t, &a)
	second.decode(t, &b)
	if second.status != http.StatusCreated || a.ID != b.ID {
		t.Fatalf("retry after lost record: %d, entry %s vs %s", second.status, b.ID, a.ID)
	}
	if got := h.account(alice.ID).Balance; got != 500 {
		t.Fatalf("balance %d, want 500", got)
	}
}

func TestInsufficientFundsIsStructuredAndReplayed(t *testing.T) {
	h := newHarness(t)
	bank := h.openAccount("Settlement", "asset", "USD")
	alice := h.openAccount("Alice", "liability", "USD")
	bob := h.openAccount("Bob", "liability", "USD")

	res := h.transfer("pay-1", alice.ID, bob.ID, 1)
	if res.status != http.StatusUnprocessableEntity || res.errorCode(t) != "insufficient_funds" {
		t.Fatalf("got %d %s", res.status, res.body)
	}
	var body ErrorBody
	res.decode(t, &body)
	if body.Error.RequestID == "" || body.Error.Message == "" {
		t.Fatalf("error body incomplete: %s", res.body)
	}
	h.transfer("fund", bank.ID, alice.ID, 10)
	// Same key, same request: the stored rejection is returned, not re-evaluated.
	if again := h.transfer("pay-1", alice.ID, bob.ID, 1); again.status != http.StatusUnprocessableEntity || again.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replayed rejection: %d %s", again.status, again.body)
	}
	if ok := h.transfer("pay-2", alice.ID, bob.ID, 1); ok.status != http.StatusCreated {
		t.Fatalf("new key: %d %s", ok.status, ok.body)
	}
}

func TestHoldCaptureAndReversal(t *testing.T) {
	h := newHarness(t)
	bank := h.openAccount("Settlement", "asset", "USD")
	alice := h.openAccount("Alice", "liability", "USD")
	shop := h.openAccount("Shop", "liability", "USD")
	h.transfer("fund", bank.ID, alice.ID, 5_000)

	res := h.do("POST", "/v1/holds", "auth-1", map[string]any{
		"debit_account_id": alice.ID, "credit_account_id": shop.ID, "amount": 3_000, "currency": "USD", "ttl_seconds": 600,
	})
	if res.status != http.StatusCreated {
		t.Fatalf("hold: %d %s", res.status, res.body)
	}
	var hold holdResponse
	res.decode(t, &hold)
	if a := h.account(alice.ID); a.Available != 2_000 || a.Balance != 5_000 {
		t.Fatalf("after hold: available %d balance %d", a.Available, a.Balance)
	}
	if over := h.transfer("too-much", alice.ID, shop.ID, 2_001); over.status != http.StatusUnprocessableEntity {
		t.Fatalf("held funds must not be spendable: %d", over.status)
	}

	path := fmt.Sprintf("/v1/holds/%s/capture", hold.ID)
	cap1 := h.do("POST", path, "cap-1", map[string]any{"amount": 2_500})
	cap2 := h.do("POST", path, "cap-1", map[string]any{"amount": 2_500})
	if cap1.status != http.StatusOK || !bytes.Equal(cap1.body, cap2.body) {
		t.Fatalf("capture: %d %s / %s", cap1.status, cap1.body, cap2.body)
	}
	var captured captureResponse
	cap1.decode(t, &captured)
	if a := h.account(alice.ID); a.Balance != 2_500 || a.Available != 2_500 {
		t.Fatalf("after capture: balance %d available %d", a.Balance, a.Available)
	}

	rev := h.do("POST", fmt.Sprintf("/v1/entries/%s/reverse", captured.Entry.ID), "rev-1", map[string]any{"reason": "refund"})
	if rev.status != http.StatusCreated {
		t.Fatalf("reverse: %d %s", rev.status, rev.body)
	}
	if a := h.account(alice.ID); a.Balance != 5_000 {
		t.Fatalf("after reversal: %d", a.Balance)
	}
	var orig entryResponse
	h.do("GET", "/v1/entries/"+captured.Entry.ID.String(), "", nil).decode(t, &orig)
	if orig.ReversedBy == nil {
		t.Fatal("original entry must point at its reversal")
	}
	if dup := h.do("POST", fmt.Sprintf("/v1/entries/%s/reverse", captured.Entry.ID), "rev-2", nil); dup.status != http.StatusConflict || dup.errorCode(t) != "already_reversed" {
		t.Fatalf("double reversal: %d %s", dup.status, dup.body)
	}
}

func TestStatementPagination(t *testing.T) {
	h := newHarness(t)
	bank := h.openAccount("Settlement", "asset", "USD")
	alice := h.openAccount("Alice", "liability", "USD")
	for i := 0; i < 5; i++ {
		h.transfer(fmt.Sprintf("d-%d", i), bank.ID, alice.ID, int64(100*(i+1)))
	}
	runner := &projection.Runner{Name: "balances", Store: h.store, Backend: h.proj}
	if err := runner.CatchUp(context.Background()); err != nil {
		t.Fatal(err)
	}

	var bal struct {
		Balance int64 `json:"balance"`
		Credits int64 `json:"credits"`
	}
	h.do("GET", "/v1/accounts/"+alice.ID.String()+"/balance", "", nil).decode(t, &bal)
	if bal.Balance != 1_500 || bal.Credits != 1_500 {
		t.Fatalf("projected balance %+v", bal)
	}

	var all []int64
	cursor := ""
	for page := 0; page < 10; page++ {
		var st statementResponse
		res := h.do("GET", "/v1/accounts/"+alice.ID.String()+"/statement?limit=2"+cursor, "", nil)
		if res.status != http.StatusOK {
			t.Fatalf("statement: %d %s", res.status, res.body)
		}
		res.decode(t, &st)
		for _, l := range st.Lines {
			all = append(all, l.BalanceAfter)
		}
		if st.NextCursor == nil {
			break
		}
		cursor = fmt.Sprintf("&cursor=%d", *st.NextCursor)
	}
	want := []int64{100, 300, 600, 1000, 1500}
	if fmt.Sprint(all) != fmt.Sprint(want) {
		t.Fatalf("running balances %v, want %v", all, want)
	}
}

func TestRequestValidation(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		name, method, path, key string
		body                    any
		status                  int
		code                    string
	}{
		{"unknown field", "POST", "/v1/accounts", "k1", `{"name":"x","type":"asset","currency":"USD","color":"red"}`, 400, codeMalformed},
		{"bad json", "POST", "/v1/entries", "k2", `{"lines":`, 400, codeMalformed},
		{"unbalanced", "POST", "/v1/entries", "k3", map[string]any{"currency": "USD", "lines": []map[string]any{
			{"account_id": uuid.New(), "side": "debit", "amount": 5}, {"account_id": uuid.New(), "side": "credit", "amount": 4}}}, 422, "unbalanced_entry"},
		{"bad currency", "POST", "/v1/accounts", "k4", map[string]any{"name": "x", "type": "asset", "currency": "XYZ"}, 400, "validation_failed"},
		{"bad id", "GET", "/v1/accounts/nope", "", nil, 400, codeMalformed},
		{"missing", "GET", "/v1/accounts/" + uuid.NewString(), "", nil, 404, "not_found"},
		{"bad limit", "GET", "/v1/accounts/" + uuid.NewString() + "/statement?limit=0", "", nil, 400, codeMalformed},
		{"hold without expiry", "POST", "/v1/holds", "k5", map[string]any{"debit_account_id": uuid.New(), "credit_account_id": uuid.New(), "amount": 1, "currency": "USD"}, 400, "validation_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := h.do(tc.method, tc.path, tc.key, tc.body)
			if res.status != tc.status || res.errorCode(t) != tc.code {
				t.Fatalf("got %d %s, want %d %s", res.status, res.body, tc.status, tc.code)
			}
		})
	}

	req, _ := http.NewRequest("POST", h.server.URL+"/v1/accounts", strings.NewReader("name=x"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Idempotency-Key", "form")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("form body: %d", res.StatusCode)
	}
}

func TestHealthAndRequestID(t *testing.T) {
	h := newHarness(t)
	res := h.do("GET", "/healthz", "", nil)
	if res.status != http.StatusOK || res.header.Get("X-Request-ID") == "" {
		t.Fatalf("healthz: %d, request id %q", res.status, res.header.Get("X-Request-ID"))
	}
	if res := h.do("GET", "/readyz", "", nil); res.status != http.StatusOK {
		t.Fatalf("readyz: %d", res.status)
	}
}
