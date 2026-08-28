package teams

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"
)

// encryptLikeGraph reproduces what Microsoft Graph does when sending a
// notification, so the test exercises the real contract rather than a mock.
func encryptLikeGraph(t *testing.T, pub *rsa.PublicKey, plaintext []byte) EncryptedContent {
	t.Helper()

	symKey := make([]byte, 32)
	if _, err := rand.Read(symKey); err != nil {
		t.Fatalf("generate symmetric key: %v", err)
	}

	block, err := aes.NewCipher(symKey)
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	padded := pkcs7Pad(plaintext, block.BlockSize())
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, symKey[:16]).CryptBlocks(ciphertext, padded)

	mac := hmac.New(sha256.New, symKey)
	mac.Write(ciphertext)

	wrapped, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, symKey, nil)
	if err != nil {
		t.Fatalf("wrap symmetric key: %v", err)
	}

	return EncryptedContent{
		Data:          base64.StdEncoding.EncodeToString(ciphertext),
		DataSignature: base64.StdEncoding.EncodeToString(mac.Sum(nil)),
		DataKey:       base64.StdEncoding.EncodeToString(wrapped),
	}
}

func pkcs7Pad(b []byte, blockSize int) []byte {
	n := blockSize - len(b)%blockSize
	out := make([]byte, len(b)+n)
	copy(out, b)
	for i := len(b); i < len(out); i++ {
		out[i] = byte(n) //nolint:gosec // blockSize is 16, so n is in range [1..16]
	}
	return out
}

func testKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

func TestDecryptRoundTrip(t *testing.T) {
	key := testKey(t)
	want := []byte(`{"id":"1565293727947","body":{"content":"hello"}}`)

	got, err := Decrypt(key, encryptLikeGraph(t, &key.PublicKey, want))
	if err != nil {
		t.Fatalf("Decrypt(): %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("Decrypt() = %q, want %q", got, want)
	}
}

func TestDecryptHandlesBlockAlignedPlaintext(t *testing.T) {
	key := testKey(t)
	want := []byte("0123456789abcdef") // exactly one AES block

	got, err := Decrypt(key, encryptLikeGraph(t, &key.PublicKey, want))
	if err != nil {
		t.Fatalf("Decrypt(): %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("Decrypt() = %q, want %q", got, want)
	}
}

func TestDecryptRejectsTamperedPayload(t *testing.T) {
	key := testKey(t)
	enc := encryptLikeGraph(t, &key.PublicKey, []byte("original"))

	raw, err := base64.StdEncoding.DecodeString(enc.Data)
	if err != nil {
		t.Fatalf("decode data: %v", err)
	}
	raw[0] ^= 0xFF
	enc.Data = base64.StdEncoding.EncodeToString(raw)

	if _, err := Decrypt(key, enc); !errors.Is(err, ErrSignatureMismatch) {
		t.Fatalf("Decrypt(tampered) error = %v, want ErrSignatureMismatch", err)
	}
}

func TestDecryptRejectsWrongKey(t *testing.T) {
	key := testKey(t)
	other := testKey(t)
	enc := encryptLikeGraph(t, &key.PublicKey, []byte("secret"))

	if _, err := Decrypt(other, enc); err == nil {
		t.Fatal("Decrypt(wrong key) error = nil, want an error")
	}
}

func TestDecryptRejectsMalformedBase64(t *testing.T) {
	key := testKey(t)
	if _, err := Decrypt(key, EncryptedContent{DataKey: "!!!not base64!!!"}); err == nil {
		t.Fatal("Decrypt(malformed) error = nil, want an error")
	}
}
