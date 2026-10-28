// Package ledger implements the application service: it loads aggregates,
// runs commands against them and commits the resulting events atomically.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/eventstore"
)

// Options configures a Service.
type Options struct {
	SnapshotEvery int64
	OutboxTopic   string
	MaxRetries    int
	Now           func() time.Time
	Logger        *slog.Logger
}

// Service is the entry point for every ledger command and query.
type Service struct {
	repo       *Repository
	topic      string
	maxRetries int
	now        func() time.Time
	log        *slog.Logger
}

// NewService wires a service on top of an event store.
func NewService(store eventstore.Store, opts Options) *Service {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 5
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	return &Service{
		repo:       NewRepository(store, opts.SnapshotEvery, opts.Logger),
		topic:      opts.OutboxTopic,
		maxRetries: opts.MaxRetries,
		now:        func() time.Time { return opts.Now().UTC() },
		log:        opts.Logger,
	}
}

// Repository exposes read access to aggregates.
func (s *Service) Repository() *Repository { return s.repo }

// Now returns the service clock.
func (s *Service) Now() time.Time { return s.now() }

// retry re-runs fn when another writer won the optimistic concurrency race.
func (s *Service) retry(ctx context.Context, op string, fn func() error) error {
	var err error
	for attempt := 1; attempt <= s.maxRetries; attempt++ {
		if err = fn(); !errors.Is(err, eventstore.ErrConcurrency) {
			return err
		}
		s.log.DebugContext(ctx, "version conflict, retrying", "op", op, "attempt", attempt)
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return domain.Errorf(domain.CodeConflict, "%s: too much contention, retry later", op)
}

func newID(id uuid.UUID) uuid.UUID {
	if id != uuid.Nil {
		return id
	}
	return uuid.New()
}

// OpenAccountCmd opens a new account.
type OpenAccountCmd struct {
	ID       uuid.UUID
	Name     string
	Type     domain.AccountType
	Currency string
	Policy   domain.Policy
}

func (s *Service) OpenAccount(ctx context.Context, cmd OpenAccountCmd) (*domain.Account, error) {
	id := newID(cmd.ID)
	acct, err := domain.OpenAccount(id, cmd.Name, cmd.Type, cmd.Currency, cmd.Policy, s.now())
	if err != nil {
		return nil, err
	}
	u := &unitOfWork{}
	u.track(domain.AccountStream(id), acct)
	if err := s.repo.commit(ctx, u, s.topic, s.now()); err != nil {
		return nil, err
	}
	return acct, nil
}

// CloseAccount closes an empty account.
func (s *Service) CloseAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	var out *domain.Account
	err := s.retry(ctx, "close_account", func() error {
		acct, err := s.repo.Account(ctx, id)
		if err != nil {
			return err
		}
		if err := acct.Close(s.now()); err != nil {
			return err
		}
		u := &unitOfWork{}
		u.track(domain.AccountStream(id), acct)
		out = acct
		return s.repo.commit(ctx, u, s.topic, s.now())
	})
	return out, err
}

// PostEntryCmd posts an arbitrary balanced journal entry.
type PostEntryCmd struct {
	ID          uuid.UUID
	Kind        domain.EntryKind
	Currency    string
	Description string
	Lines       []domain.Line
}

func (s *Service) PostEntry(ctx context.Context, cmd PostEntryCmd) (*domain.Entry, error) {
	if cmd.Kind == domain.KindReversal || cmd.Kind == domain.KindCapture {
		return nil, domain.Errorf(domain.CodeValidation, "kind %q is reserved", cmd.Kind)
	}
	params := domain.EntryParams{
		ID: newID(cmd.ID), Kind: cmd.Kind, Currency: cmd.Currency,
		Description: cmd.Description, Lines: cmd.Lines,
	}
	var out *domain.Entry
	err := s.retry(ctx, "post_entry", func() error {
		now := s.now()
		entry, err := domain.NewEntry(params, now)
		if err != nil {
			return err
		}
		u := &unitOfWork{}
		u.track(domain.EntryStream(entry.ID), entry)
		if err := s.postLines(ctx, u, entry, now); err != nil {
			return err
		}
		out = entry
		return s.repo.commit(ctx, u, s.topic, now)
	})
	return out, err
}

// TransferCmd moves an amount from the credited account to the debited one
// as a two-line entry.
type TransferCmd struct {
	ID              uuid.UUID
	DebitAccountID  uuid.UUID
	CreditAccountID uuid.UUID
	Amount          int64
	Currency        string
	Description     string
}

func (s *Service) Transfer(ctx context.Context, cmd TransferCmd) (*domain.Entry, error) {
	return s.PostEntry(ctx, PostEntryCmd{
		ID: cmd.ID, Kind: domain.KindTransfer, Currency: cmd.Currency, Description: cmd.Description,
		Lines: []domain.Line{
			{AccountID: cmd.DebitAccountID, Side: domain.Debit, Amount: cmd.Amount},
			{AccountID: cmd.CreditAccountID, Side: domain.Credit, Amount: cmd.Amount},
		},
	})
}

// postLines applies every line of entry to its account. Accounts are loaded
// in ID order so that concurrent writers contend predictably.
func (s *Service) postLines(ctx context.Context, u *unitOfWork, entry *domain.Entry, now time.Time) error {
	lines := append([]domain.Line(nil), entry.Lines...)
	sort.Slice(lines, func(i, j int) bool { return lines[i].AccountID.String() < lines[j].AccountID.String() })
	for _, l := range lines {
		acct, err := s.repo.Account(ctx, l.AccountID)
		if err != nil {
			return err
		}
		if acct.Currency != entry.Currency {
			return domain.Errorf(domain.CodeCurrencyMismatch, "account %s is %s, entry is %s", acct.ID, acct.Currency, entry.Currency)
		}
		if err := acct.Post(entry.ID, l.Side, l.Amount, entry.Description, now); err != nil {
			return err
		}
		u.track(domain.AccountStream(acct.ID), acct)
	}
	return nil
}

// ReverseCmd posts the mirror image of an existing entry.
type ReverseCmd struct {
	EntryID    uuid.UUID
	ReversalID uuid.UUID
	Reason     string
}

func (s *Service) Reverse(ctx context.Context, cmd ReverseCmd) (*domain.Entry, error) {
	reversalID := newID(cmd.ReversalID)
	var out *domain.Entry
	err := s.retry(ctx, "reverse_entry", func() error {
		now := s.now()
		original, err := s.repo.Entry(ctx, cmd.EntryID)
		if err != nil {
			return err
		}
		if err := original.MarkReversed(reversalID, cmd.Reason, now); err != nil {
			return err
		}
		origID := original.ID
		reversal, err := domain.NewEntry(domain.EntryParams{
			ID: reversalID, Kind: domain.KindReversal, Currency: original.Currency,
			Description: fmt.Sprintf("Reversal of %s", original.ID), Lines: original.ReversalLines(), Reverses: &origID,
		}, now)
		if err != nil {
			return err
		}
		u := &unitOfWork{}
		u.track(domain.EntryStream(original.ID), original)
		u.track(domain.EntryStream(reversal.ID), reversal)
		if err := s.postLines(ctx, u, reversal, now); err != nil {
			return err
		}
		out = reversal
		return s.repo.commit(ctx, u, s.topic, now)
	})
	return out, err
}

// PlaceHoldCmd authorizes a pending transfer.
type PlaceHoldCmd struct {
	ID              uuid.UUID
	DebitAccountID  uuid.UUID
	CreditAccountID uuid.UUID
	Amount          int64
	Currency        string
	Description     string
	ExpiresAt       time.Time
}

func (s *Service) PlaceHold(ctx context.Context, cmd PlaceHoldCmd) (*domain.Hold, error) {
	id := newID(cmd.ID)
	var out *domain.Hold
	err := s.retry(ctx, "place_hold", func() error {
		now := s.now()
		hold, err := domain.PlaceHold(id, cmd.DebitAccountID, cmd.CreditAccountID, cmd.Currency, cmd.Amount, cmd.Description, cmd.ExpiresAt, now)
		if err != nil {
			return err
		}
		u := &unitOfWork{}
		u.track(domain.HoldStream(id), hold)
		for _, leg := range holdLegs(hold) {
			acct, err := s.repo.Account(ctx, leg.account)
			if err != nil {
				return err
			}
			if acct.Currency != hold.Currency {
				return domain.Errorf(domain.CodeCurrencyMismatch, "account %s is %s, hold is %s", acct.ID, acct.Currency, hold.Currency)
			}
			if err := acct.Reserve(id, leg.side, hold.Amount, hold.ExpiresAt, now); err != nil {
				return err
			}
			u.track(domain.AccountStream(acct.ID), acct)
		}
		out = hold
		return s.repo.commit(ctx, u, s.topic, now)
	})
	return out, err
}

// CaptureHoldCmd settles a hold. Amount 0 captures the full held amount.
type CaptureHoldCmd struct {
	HoldID  uuid.UUID
	EntryID uuid.UUID
	Amount  int64
}

// CaptureResult is the hold after capture and the entry it produced.
type CaptureResult struct {
	Hold  *domain.Hold
	Entry *domain.Entry
}

func (s *Service) CaptureHold(ctx context.Context, cmd CaptureHoldCmd) (*CaptureResult, error) {
	entryID := newID(cmd.EntryID)
	var out *CaptureResult
	err := s.retry(ctx, "capture_hold", func() error {
		now := s.now()
		hold, err := s.repo.Hold(ctx, cmd.HoldID)
		if err != nil {
			return err
		}
		amount := cmd.Amount
		if amount == 0 {
			amount = hold.Amount
		}
		if err := hold.Capture(entryID, amount, now); err != nil {
			return err
		}
		holdID := hold.ID
		entry, err := domain.NewEntry(domain.EntryParams{
			ID: entryID, Kind: domain.KindCapture, Currency: hold.Currency, Description: hold.Description,
			HoldID: &holdID,
			Lines: []domain.Line{
				{AccountID: hold.DebitAccountID, Side: domain.Debit, Amount: amount},
				{AccountID: hold.CreditAccountID, Side: domain.Credit, Amount: amount},
			},
		}, now)
		if err != nil {
			return err
		}
		u := &unitOfWork{}
		u.track(domain.HoldStream(hold.ID), hold)
		u.track(domain.EntryStream(entry.ID), entry)
		for _, leg := range holdLegs(hold) {
			acct, err := s.repo.Account(ctx, leg.account)
			if err != nil {
				return err
			}
			if err := acct.Settle(hold.ID, entry.ID, amount, hold.Description, now); err != nil {
				return err
			}
			u.track(domain.AccountStream(acct.ID), acct)
		}
		out = &CaptureResult{Hold: hold, Entry: entry}
		return s.repo.commit(ctx, u, s.topic, now)
	})
	return out, err
}

// VoidHold releases a pending hold without moving money.
func (s *Service) VoidHold(ctx context.Context, holdID uuid.UUID, reason string) (*domain.Hold, error) {
	var out *domain.Hold
	err := s.retry(ctx, "void_hold", func() error {
		now := s.now()
		hold, err := s.repo.Hold(ctx, holdID)
		if err != nil {
			return err
		}
		if err := hold.Void(reason, now); err != nil {
			return err
		}
		u := &unitOfWork{}
		u.track(domain.HoldStream(hold.ID), hold)
		for _, leg := range holdLegs(hold) {
			acct, err := s.repo.Account(ctx, leg.account)
			if err != nil {
				return err
			}
			if err := acct.Release(hold.ID); err != nil {
				return err
			}
			u.track(domain.AccountStream(acct.ID), acct)
		}
		out = hold
		return s.repo.commit(ctx, u, s.topic, now)
	})
	return out, err
}

type leg struct {
	account uuid.UUID
	side    domain.Side
}

func holdLegs(h *domain.Hold) []leg {
	legs := []leg{{h.DebitAccountID, domain.Debit}, {h.CreditAccountID, domain.Credit}}
	sort.Slice(legs, func(i, j int) bool { return legs[i].account.String() < legs[j].account.String() })
	return legs
}

func (s *Service) GetAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	return s.repo.Account(ctx, id)
}

func (s *Service) GetEntry(ctx context.Context, id uuid.UUID) (*domain.Entry, error) {
	return s.repo.Entry(ctx, id)
}

func (s *Service) GetHold(ctx context.Context, id uuid.UUID) (*domain.Hold, error) {
	return s.repo.Hold(ctx, id)
}
