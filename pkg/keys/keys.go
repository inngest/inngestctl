package keys

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// A representation of a signing key that is used to sign events.
type SigningKey struct {
	// The raw key provided when creating the key.
	raw string

	// The hash of the key. This will always be present even if the raw value
	// is deemed invalid.
	hash string

	// Whether the key is valid or not. A key is valid if it is a non-empty
	// hex-encoded string.
	valid bool
}

// NewSigningKey creates a new SigningKey with the given key. If there was an
// error creating the key, it will still return a SigningKey but the key will be
// invalid.
func NewSigningKey(key string) (*SigningKey, error) {
	k := SigningKey{}
	err := k.Set(key)

	return &k, err
}

// Set a new key for the SigningKey. This will update the hash and validity of
// the key.
func (k *SigningKey) Set(key string) error {
	k.valid = false
	k.hash = ""
	k.raw = key

	hash, err := getHash(key)
	if err != nil {
		return fmt.Errorf("failed to set key: %w", err)
	}

	k.hash = hash
	k.valid = k.isValid()

	return nil
}

// Raw returns the raw key.
func (k *SigningKey) Raw() string {
	return k.raw
}

// Hash returns the hash of the key.
func (k *SigningKey) Hash() string {
	return k.hash
}

// Valid returns whether the key is valid or not. A key is valid if it is a
// non-empty hex-encoded string.
func (k *SigningKey) Valid() bool {
	return k.valid
}

// Empty returns whether the key is empty or not.
func (k *SigningKey) Empty() bool {
	return k.raw == ""
}

// Equal returns whether the key is equal to the given key. It compares the
// hashed value of the key.
func (k *SigningKey) Equal(hashedKey string) bool {
	return k.hash == hashedKey
}

// isValid checks whether the key is valid or not. A key is valid if it is a
// non-empty hex-encoded string.
func (k *SigningKey) isValid() bool {
	if k.raw == "" || k.hash == "" {
		return false
	}

	// Check if the key is a valid hex-encoded string.
	_, err := hex.DecodeString(k.raw)
	return err == nil
}

// getHash hashes the given key. If the key is not a valid hex-encoded string,
// it will return an error.
func getHash(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("key is empty")
	}

	decodedKey, err := hex.DecodeString(key)
	if err != nil {
		return "", fmt.Errorf("key is not a valid hex-encoded string: %w", err)
	}

	sum := sha256.Sum256(decodedKey)

	return hex.EncodeToString(sum[:]), nil
}
