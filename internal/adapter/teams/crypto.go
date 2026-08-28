// Package teams implements the Microsoft Teams side of the bridge.
package teams

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // mandated by the Graph OAEP contract
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
)

// ErrSignatureMismatch means the payload failed its HMAC check and must not be
// decrypted. Treat it as tampering and investigate.
var ErrSignatureMismatch = errors.New("teams: resource data signature mismatch")

// EncryptedContent is the encryptedContent object of a change notification.
type EncryptedContent struct {
	Data                            string `json:"data"`
	DataSignature                   string `json:"dataSignature"`
	DataKey                         string `json:"dataKey"`
	EncryptionCertificateID         string `json:"encryptionCertificateId"`
	EncryptionCertificateThumbprint string `json:"encryptionCertificateThumbprint"`
}

// Decrypt recovers the JSON resource payload from a change notification.
//
// The algorithm is fixed by Microsoft Graph:
//  1. Unwrap the single-use symmetric key from dataKey with RSA-OAEP (SHA-1).
//  2. Verify HMAC-SHA256 of the raw ciphertext against dataSignature.
//  3. Decrypt with AES-256-CBC, PKCS7 padding, IV = first 16 bytes of the key.
func Decrypt(priv *rsa.PrivateKey, enc EncryptedContent) ([]byte, error) {
	wrappedKey, err := base64.StdEncoding.DecodeString(enc.DataKey)
	if err != nil {
		return nil, fmt.Errorf("decode dataKey: %w", err)
	}
	symKey, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, priv, wrappedKey, nil) //nolint:gosec // Graph contract
	if err != nil {
		return nil, fmt.Errorf("unwrap symmetric key: %w", err)
	}

	ciphertext, err := base64.StdEncoding.DecodeString(enc.Data)
	if err != nil {
		return nil, fmt.Errorf("decode data: %w", err)
	}
	expectedSig, err := base64.StdEncoding.DecodeString(enc.DataSignature)
	if err != nil {
		return nil, fmt.Errorf("decode dataSignature: %w", err)
	}

	mac := hmac.New(sha256.New, symKey)
	mac.Write(ciphertext)
	if !hmac.Equal(mac.Sum(nil), expectedSig) {
		return nil, ErrSignatureMismatch
	}

	block, err := aes.NewCipher(symKey)
	if err != nil {
		return nil, fmt.Errorf("new cipher: %w", err)
	}
	if len(symKey) < block.BlockSize() {
		return nil, errors.New("teams: symmetric key shorter than the AES block size")
	}
	if len(ciphertext) == 0 || len(ciphertext)%block.BlockSize() != 0 {
		return nil, errors.New("teams: ciphertext is not a multiple of the AES block size")
	}

	plaintext := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, symKey[:block.BlockSize()]).CryptBlocks(plaintext, ciphertext)

	return pkcs7Unpad(plaintext, block.BlockSize())
}

func pkcs7Unpad(b []byte, blockSize int) ([]byte, error) {
	if len(b) == 0 {
		return nil, errors.New("teams: empty plaintext")
	}
	pad := int(b[len(b)-1])
	if pad == 0 || pad > blockSize || pad > len(b) {
		return nil, errors.New("teams: invalid PKCS7 padding")
	}
	for _, c := range b[len(b)-pad:] {
		if int(c) != pad {
			return nil, errors.New("teams: invalid PKCS7 padding")
		}
	}
	return b[:len(b)-pad], nil
}
