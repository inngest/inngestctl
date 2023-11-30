package headers

import (
	"net/http"
	"strings"
)

const (
	HeaderKeyContentType = "Content-Type"

	// Inngest environment name
	HeaderKeyEnv = "X-Inngest-Env"

	// SDK version
	HeaderKeySDK = "X-Inngest-SDK"

	// Tells the consumers (e.g. SDKs) what kind of Inngest server they're
	// communicating with (Cloud or Dev Server).
	HeaderKeyServerKind = "X-Inngest-Server-Kind"

	// Used by an SDK to tell the Inngest server what kind the SDK expects it
	// to be, used to validate that every part of a registration is performed
	// against the same target.
	HeaderKeyExpectedServerKind = "X-Inngest-Expected-Server-Kind"

	// SDK uses this to tell the Inngest server whether to skip retrying
	HeaderKeyNoRetry = "X-Inngest-No-Retry"

	HeaderKeyRequestVersion = "X-Inngest-Req-Version"
	HeaderKeyRetryAfter     = "Retry-After"
	HeaderKeySignature      = "X-Inngest-Signature"
)

const (
	ServerKindCloud = "cloud"
	ServerKindDev   = "dev"
)

// ValueFromMap returns the value of the header with the given case insensitive
// key. Returns an empty string if the header doesn't exist.
func ValueFromMap(key string, m map[string]string) string {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v
		}
	}

	return ""
}

func StaticHeadersMiddleware(serverKind string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(HeaderKeyServerKind, serverKind)
			next.ServeHTTP(w, r)
		})
	}
}
