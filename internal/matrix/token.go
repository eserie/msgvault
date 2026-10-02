package matrix

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
	"go.kenn.io/kit/atomicfile"
	"go.kenn.io/msgvault/internal/fileutil"
)

// Credentials are the durable credentials for one dedicated Matrix device.
type Credentials struct {
	Homeserver  string `json:"homeserver"`
	UserID      string `json:"user_id"`
	DeviceID    string `json:"device_id"`
	AccessToken string `json:"access_token"`
}

var secureReplaceCredentials = fileutil.SecureReplaceFile

// WithCredentialLifecycleLock serializes Matrix device creation and removal
// across processes. The callback must re-check both credential and source
// state after acquiring the lock.
func WithCredentialLifecycleLock(tokensDir string, fn func() error) (retErr error) {
	if fn == nil {
		return errors.New("matrix credential lifecycle operation is missing")
	}
	if err := fileutil.SecureMkdirAll(tokensDir, 0o700); err != nil {
		return fmt.Errorf("create tokens dir: %w", err)
	}
	lifecycleLock := flock.New(filepath.Join(tokensDir, ".matrix-lifecycle.lock"), flock.SetPermissions(0o600))
	if err := lifecycleLock.Lock(); err != nil {
		return fmt.Errorf("lock Matrix credential lifecycle: %w", err)
	}
	defer func() {
		if err := lifecycleLock.Unlock(); err != nil && retErr == nil {
			retErr = fmt.Errorf("unlock Matrix credential lifecycle: %w", err)
		}
	}()
	return fn()
}

func accountKey(userID string) string {
	sum := sha256.Sum256([]byte(userID))
	return hex.EncodeToString(sum[:12])
}

func tokenPath(tokensDir, userID string) string {
	return filepath.Join(tokensDir, "matrix_"+accountKey(userID)+".json")
}

// SaveCredentials atomically writes credentials to a 0600 file.
func SaveCredentials(tokensDir string, creds Credentials) error {
	return saveCredentials(tokensDir, creds, false)
}

// SaveNewCredentials writes credentials created by add-matrix. A failure
// reported after publication removes the new file because the caller will log
// out that fresh device. Updates and cleanup recovery use SaveCredentials so a
// published access token is never discarded while its device may remain live.
func SaveNewCredentials(tokensDir string, creds Credentials) error {
	return saveCredentials(tokensDir, creds, true)
}

func saveCredentials(tokensDir string, creds Credentials, removePublishedOnError bool) error {
	if err := fileutil.SecureMkdirAll(tokensDir, 0o700); err != nil {
		return fmt.Errorf("create tokens dir: %w", err)
	}
	data, err := json.Marshal(creds, json.Deterministic(true))
	if err != nil {
		return fmt.Errorf("encode Matrix credentials: %w", err)
	}
	path := tokenPath(tokensDir, creds.UserID)
	if err := secureReplaceCredentials(path, data, 0o600); err != nil {
		// SecureReplaceFile can report an error after publishing the replacement
		// (for example, while syncing its directory). Do not leave credentials
		// behind for a device that add-matrix will log out on this error path.
		if removePublishedOnError && errors.Is(err, atomicfile.ErrPublished) {
			if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
				return fmt.Errorf("write Matrix credentials: %w (remove incomplete credentials: %w)", err, removeErr)
			}
		}
		return fmt.Errorf("write Matrix credentials: %w", err)
	}
	return nil
}

// LoadCredentials loads and identity-checks one Matrix credential file.
func LoadCredentials(tokensDir, userID string) (Credentials, error) {
	data, err := os.ReadFile(tokenPath(tokensDir, userID))
	if err != nil {
		if os.IsNotExist(err) {
			return Credentials{}, fmt.Errorf("no Matrix credentials for %s (run 'add-matrix' first)", userID)
		}
		return Credentials{}, fmt.Errorf("read Matrix credentials: %w", err)
	}
	var creds Credentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return Credentials{}, fmt.Errorf("parse Matrix credentials: %w", err)
	}
	if creds.UserID != userID || creds.Homeserver == "" || creds.DeviceID == "" || creds.AccessToken == "" {
		return Credentials{}, fmt.Errorf("matrix credential file for %s is incomplete or belongs to %s", userID, creds.UserID)
	}
	return creds, nil
}

// DeleteCredentials removes the dedicated device credential for an account.
func DeleteCredentials(tokensDir, userID string) error {
	err := os.Remove(tokenPath(tokensDir, userID))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// CredentialsExist reports whether an account already has a dedicated device.
func CredentialsExist(tokensDir, userID string) (bool, error) {
	_, err := os.Stat(tokenPath(tokensDir, userID))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("check Matrix credentials: %w", err)
}
