package utils

import (
	"encoding/json"

	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/xxtea/xxtea-go/xxtea"
)

// EncryptCursor encrypts the cursor
func EncryptCursor(input []types.FieldValue, key string) (string, error) {
	// Serialize the input to JSON
	jsonData, err := json.Marshal(input)
	if err != nil {
		return "", err
	}

	// Encrypt the JSON data using the XXTEA algorithm
	encryptedBytes := xxtea.Encrypt(jsonData, []byte(key))

	// Encode the encrypted bytes to a base58 string
	encoded := base58.Encode(encryptedBytes)
	return encoded, nil
}

// DecryptCursor decrypts the cursor
func DecryptCursor(input string, key string) ([]types.FieldValue, error) {
	// Decode the base58 string to get the encrypted bytes
	decoded := base58.Decode(input)

	// Decrypt the JSON data using the XXTEA algorithm
	decryptedBytes := xxtea.Decrypt(decoded, []byte(key))

	// Unmarshal the decrypted JSON back into an array
	var arr []types.FieldValue
	if err := json.Unmarshal(decryptedBytes, &arr); err != nil {
		return nil, err
	}
	return arr, nil
}
