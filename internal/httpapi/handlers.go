package httpapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/ledger"
	"github.com/MahyarNemati/ledger/internal/projection"
)

func (s *server) openAccount(req request) (result, error) {
	var in openAccountRequest
	if err := decode(req.body, &in); err != nil {
		return result{}, err
	}
	ctx := req.Context()
	a, err := s.Service.OpenAccount(ctx, ledger.OpenAccountCmd{
		ID: req.id, Name: in.Name, Type: in.Type, Currency: in.Currency,
		Policy: domain.Policy{AllowOverdraft: in.AllowOverdraft, OverdraftLimit: in.OverdraftLimit},
	})
	if domain.IsCode(err, domain.CodeAlreadyExists) {
		a, err = s.Service.GetAccount(ctx, req.id)
	}
	if err != nil {
		return result{}, err
	}
	return result{http.StatusCreated, toAccount(a, s.Service.Now())}, nil
}

func (s *server) getAccount(r *http.Request) (result, error) {
	id, err := pathID(r)
	if err != nil {
		return result{}, err
	}
	a, err := s.Service.GetAccount(r.Context(), id)
	if err != nil {
		return result{}, err
	}
	return result{http.StatusOK, toAccount(a, s.Service.Now())}, nil
}

func (s *server) closeAccount(req request) (result, error) {
	id, err := pathID(req.Request)
	if err != nil {
		return result{}, err
	}
	a, err := s.Service.CloseAccount(req.Context(), id)
	if err != nil {
		return result{}, err
	}
	return result{http.StatusOK, toAccount(a, s.Service.Now())}, nil
}

func (s *server) getBalance(r *http.Request) (result, error) {
	id, err := pathID(r)
	if err != nil {
		return result{}, err
	}
	b, err := s.Projections.Balance(r.Context(), id)
	if errors.Is(err, projection.ErrNotFound) {
		return result{}, domain.Errorf(domain.CodeNotFound, "no projected balance for account %s yet", id)
	}
	if err != nil {
		return result{}, err
	}
	return result{http.StatusOK, balanceResponse{Balance: b, Posted: b.Posted()}}, nil
}

func (s *server) getStatement(r *http.Request) (result, error) {
	id, err := pathID(r)
	if err != nil {
		return result{}, err
	}
	after, err := queryInt(r, "cursor", 0, 0, 1<<62)
	if err != nil {
		return result{}, err
	}
	limit, err := queryInt(r, "limit", defaultPageSize, 1, maxPageSize)
	if err != nil {
		return result{}, err
	}
	lines, err := s.Projections.Statement(r.Context(), id, after, int(limit))
	if err != nil {
		return result{}, err
	}
	out := statementResponse{AccountID: id, Lines: lines}
	if out.Lines == nil {
		out.Lines = []projection.StatementLine{}
	}
	if len(lines) == int(limit) {
		next := lines[len(lines)-1].Position
		out.NextCursor = &next
	}
	return result{http.StatusOK, out}, nil
}

func (s *server) postEntry(req request) (result, error) {
	var in postEntryRequest
	if err := decode(req.body, &in); err != nil {
		return result{}, err
	}
	lines := make([]domain.Line, len(in.Lines))
	for i, l := range in.Lines {
		lines[i] = domain.Line(l)
	}
	ctx := req.Context()
	e, err := s.Service.PostEntry(ctx, ledger.PostEntryCmd{ID: req.id, Currency: in.Currency, Description: in.Description, Lines: lines})
	if domain.IsCode(err, domain.CodeAlreadyExists) {
		e, err = s.Service.GetEntry(ctx, req.id)
	}
	if err != nil {
		return result{}, err
	}
	return result{http.StatusCreated, toEntry(e)}, nil
}

func (s *server) transfer(req request) (result, error) {
	var in transferRequest
	if err := decode(req.body, &in); err != nil {
		return result{}, err
	}
	ctx := req.Context()
	e, err := s.Service.Transfer(ctx, ledger.TransferCmd{
		ID: req.id, DebitAccountID: in.DebitAccountID, CreditAccountID: in.CreditAccountID,
		Amount: in.Amount, Currency: in.Currency, Description: in.Description,
	})
	if domain.IsCode(err, domain.CodeAlreadyExists) {
		e, err = s.Service.GetEntry(ctx, req.id)
	}
	if err != nil {
		return result{}, err
	}
	return result{http.StatusCreated, toEntry(e)}, nil
}

func (s *server) getEntry(r *http.Request) (result, error) {
	id, err := pathID(r)
	if err != nil {
		return result{}, err
	}
	e, err := s.Service.GetEntry(r.Context(), id)
	if err != nil {
		return result{}, err
	}
	return result{http.StatusOK, toEntry(e)}, nil
}

func (s *server) reverseEntry(req request) (result, error) {
	id, err := pathID(req.Request)
	if err != nil {
		return result{}, err
	}
	var in reverseRequest
	if err := decode(req.body, &in); err != nil {
		return result{}, err
	}
	ctx := req.Context()
	e, err := s.Service.Reverse(ctx, ledger.ReverseCmd{EntryID: id, ReversalID: req.id, Reason: in.Reason})
	if domain.IsCode(err, domain.CodeAlreadyReversed) {
		if orig, gerr := s.Service.GetEntry(ctx, id); gerr == nil && orig.ReversedBy != nil && *orig.ReversedBy == req.id {
			e, err = s.Service.GetEntry(ctx, req.id)
		}
	}
	if err != nil {
		return result{}, err
	}
	return result{http.StatusCreated, toEntry(e)}, nil
}

func (s *server) placeHold(req request) (result, error) {
	var in placeHoldRequest
	if err := decode(req.body, &in); err != nil {
		return result{}, err
	}
	now := s.Service.Now()
	expires := now.Add(time.Duration(in.TTLSeconds) * time.Second)
	if in.ExpiresAt != nil {
		expires = *in.ExpiresAt
	} else if in.TTLSeconds <= 0 {
		return result{}, domain.Errorf(domain.CodeValidation, "either expires_at or a positive ttl_seconds is required")
	}
	ctx := req.Context()
	h, err := s.Service.PlaceHold(ctx, ledger.PlaceHoldCmd{
		ID: req.id, DebitAccountID: in.DebitAccountID, CreditAccountID: in.CreditAccountID,
		Amount: in.Amount, Currency: in.Currency, Description: in.Description, ExpiresAt: expires,
	})
	if domain.IsCode(err, domain.CodeAlreadyExists) {
		h, err = s.Service.GetHold(ctx, req.id)
	}
	if err != nil {
		return result{}, err
	}
	return result{http.StatusCreated, toHold(h, s.Service.Now())}, nil
}

func (s *server) getHold(r *http.Request) (result, error) {
	id, err := pathID(r)
	if err != nil {
		return result{}, err
	}
	h, err := s.Service.GetHold(r.Context(), id)
	if err != nil {
		return result{}, err
	}
	return result{http.StatusOK, toHold(h, s.Service.Now())}, nil
}

func (s *server) captureHold(req request) (result, error) {
	id, err := pathID(req.Request)
	if err != nil {
		return result{}, err
	}
	var in captureRequest
	if err := decode(req.body, &in); err != nil {
		return result{}, err
	}
	if in.Amount < 0 {
		return result{}, domain.Errorf(domain.CodeValidation, "amount must not be negative")
	}
	ctx := req.Context()
	res, err := s.Service.CaptureHold(ctx, ledger.CaptureHoldCmd{HoldID: id, EntryID: req.id, Amount: in.Amount})
	if domain.IsCode(err, domain.CodeHoldNotPending) {
		if h, gerr := s.Service.GetHold(ctx, id); gerr == nil && h.EntryID != nil && *h.EntryID == req.id {
			if e, gerr := s.Service.GetEntry(ctx, req.id); gerr == nil {
				res, err = &ledger.CaptureResult{Hold: h, Entry: e}, nil
			}
		}
	}
	if err != nil {
		return result{}, err
	}
	now := s.Service.Now()
	return result{http.StatusOK, captureResponse{Hold: toHold(res.Hold, now), Entry: toEntry(res.Entry)}}, nil
}

func (s *server) voidHold(req request) (result, error) {
	id, err := pathID(req.Request)
	if err != nil {
		return result{}, err
	}
	var in voidRequest
	if err := decode(req.body, &in); err != nil {
		return result{}, err
	}
	h, err := s.Service.VoidHold(req.Context(), id, in.Reason)
	if err != nil {
		return result{}, err
	}
	return result{http.StatusOK, toHold(h, s.Service.Now())}, nil
}
