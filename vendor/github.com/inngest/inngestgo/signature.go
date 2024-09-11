package inngestgo

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/inngest/inngest/pkg/logger"
)

var (
	ErrExpiredSignature = fmt.Errorf("expired signature")
	ErrInvalidSignature = fmt.Errorf("invalid signature")
	ErrInvalidTimestamp = fmt.Errorf("invalid timestamp")

	keyRegexp             = regexp.MustCompile(`^signkey-\w+-`)
	signatureTimeDeltaMax = 5 * time.Minute
)

// Sign signs a request body with the given key at the given timestamp.
func Sign(ctx context.Context, at time.Time, key, body []byte) (string, error) {
	key = normalizeKey(key)

	var err error
	if len(body) > 0 {
		body, err = jcs.Transform(body)
		if err != nil {
			logger.StdlibLogger(ctx).Warn("failed to canonicalize body", "error", err)
		}
	}

	ts := at.Unix()
	if at.IsZero() {
		ts = time.Now().Unix()
	}

	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(body)
	// Write the timestamp as a unix timestamp to the hmac to prevent
	// timing attacks.
	_, _ = mac.Write([]byte(fmt.Sprintf("%d", ts)))
	sig := hex.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("t=%d&s=%s", ts, sig), nil
}

// validateSignature ensures that the signature for the given body is signed with
// the given key within a given time period to prevent invalid requests or
// replay attacks.
func validateSignature(ctx context.Context, sig string, key, body []byte) (bool, error) {
	key = normalizeKey(key)

	val, err := url.ParseQuery(sig)
	if err != nil || (val.Get("t") == "" || val.Get("s") == "") {
		return false, ErrInvalidSignature
	}
	str, err := strconv.Atoi(val.Get("t"))
	if err != nil {
		return false, ErrInvalidTimestamp
	}
	ts := time.Unix(int64(str), 0)
	if time.Since(ts) > signatureTimeDeltaMax {
		return false, ErrExpiredSignature
	}

	actual, err := Sign(ctx, ts, key, body)
	if err != nil {
		return false, err
	}
	if actual != sig {
		return false, ErrInvalidSignature
	}

	return true, nil
}

// ValidateSignature ensures that the signature for the given body is signed with
// the given key within a given time period to prevent invalid requests or
// replay attacks. A signing key fallback is used if provided. Returns the
// correct signing key, which is useful when signing responses
func ValidateSignature(
	ctx context.Context,
	sig string,
	signingKey string,
	signingKeyFallback string,
	body []byte,
) (bool, string, error) {
	// The key that was used to sign the request
	correctKey := ""

	if IsDev() {
		return true, correctKey, nil
	}

	valid, err := validateSignature(ctx, sig, []byte(signingKey), body)
	if !valid {
		if signingKeyFallback != "" {
			// Validation failed with the primary key, so try the fallback key
			valid, err := validateSignature(ctx, sig, []byte(signingKeyFallback), body)
			if valid {
				correctKey = signingKeyFallback
			}
			return valid, correctKey, err
		}
	} else {
		correctKey = signingKey
	}

	return valid, correctKey, err
}

func normalizeKey(key []byte) []byte {
	return keyRegexp.ReplaceAll(key, []byte{})
}

func hashedSigningKey(key []byte) ([]byte, error) {
	prefix := keyRegexp.Find(key)
	key = normalizeKey(key)

	dst := make([]byte, hex.DecodedLen(len(key)))
	if _, err := hex.Decode(dst, key); err != nil {
		return nil, err
	}

	sum := sha256.Sum256(dst)
	enc := hex.EncodeToString(sum[:])
	return append(prefix, []byte(enc)...), nil
}
